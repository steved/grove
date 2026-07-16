// /*
// Copyright 2026 The Grove Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// */

package topology

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"
	"github.com/ai-dynamo/grove/operator/internal/controller/topologyresolver"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PodCliqueTopologyAffinityState contains persisted status and the placement data used in one reconciliation.
type PodCliqueTopologyAffinityState struct {
	*grovecorev1alpha1.PodCliqueTopologyAffinityStatus
	LabelKey string
}

// ResolvePodCliqueTopologyAffinity resolves topology status for a PodClique. It returns
// nil when the PodClique has no topologyAffinity.
func ResolvePodCliqueTopologyAffinity(ctx context.Context, cl client.Client, topologyResolver topologyresolver.Resolver, pcs *grovecorev1alpha1.PodCliqueSet, pclq *grovecorev1alpha1.PodClique) (*PodCliqueTopologyAffinityState, error) {
	if pclq.Spec.Affinity == nil || pclq.Spec.Affinity.TopologyAffinity == nil {
		return nil, nil
	}

	affinity := pclq.Spec.Affinity.TopologyAffinity
	level, generation, err := LevelForDomain(ctx, cl, affinity.TopologyName, affinity.Domain)
	if err != nil {
		return nil, err
	}
	allDomains, err := topologyResolver.NodeValues(ctx, level.Key)
	if err != nil {
		return nil, err
	}
	slices.Sort(allDomains)

	associatedDomains, associatedReady, err := AssociatedScheduledDomains(ctx, cl, topologyResolver, pcs, pclq, affinity, level)
	if err != nil {
		return nil, err
	}
	targetDomains := allDomains
	if associatedReady {
		targetDomains = associatedDomains
	} else if pclq.Status.TopologyAffinity != nil && pclq.Status.TopologyAffinity.AssociatedReady {
		targetDomains = slices.Clone(pclq.Status.TopologyAffinity.TargetDomains)
	}

	return &PodCliqueTopologyAffinityState{
		PodCliqueTopologyAffinityStatus: &grovecorev1alpha1.PodCliqueTopologyAffinityStatus{
			ObservedTopologyBindingGeneration: generation,
			AllDomains:                        allDomains,
			AssociatedDomains:                 associatedDomains,
			TargetDomains:                     targetDomains,
			AssociatedReady:                   associatedReady,
		},
		LabelKey: level.Key,
	}, nil
}

// AssociatedScheduledDomains returns the union of topology domains used by the
// associated cliqueNames and whether all associated PodCliques have met their
// scheduled minimum.
func AssociatedScheduledDomains(ctx context.Context, cl client.Client, topologyResolver topologyresolver.Resolver, pcs *grovecorev1alpha1.PodCliqueSet, pclq *grovecorev1alpha1.PodClique, affinity *grovecorev1alpha1.TopologyAffinity, level grovecorev1alpha1.TopologyLevel) ([]string, bool, error) {
	associatedPCLQNames, err := AssociatedPodCliqueFQNs(pcs, pclq, affinity)
	if err != nil {
		return nil, false, err
	}

	associatedDomains := sets.New[string]()
	associatedReady := true
	allScaledToZero := true
	for _, associatedPCLQName := range associatedPCLQNames {
		associatedPCLQ := &grovecorev1alpha1.PodClique{}
		if err = cl.Get(ctx, client.ObjectKey{Namespace: pclq.Namespace, Name: associatedPCLQName}, associatedPCLQ); err != nil {
			return nil, false, client.IgnoreNotFound(err)
		}
		if associatedPCLQ.Spec.Replicas != 0 {
			allScaledToZero = false
		}

		minScheduled, ok := associatedPCLQ.MinAvailable()
		if !ok || associatedPCLQ.Status.ScheduledReplicas < minScheduled {
			associatedReady = false
		}

		pods, err := componentutils.GetPCLQPods(ctx, cl, pcs.Name, associatedPCLQ)
		if err != nil {
			return nil, false, err
		}
		for _, pod := range pods {
			if pod.Spec.NodeName == "" {
				continue
			}
			domains, err := associatedPodDomains(ctx, cl, topologyResolver, pod, level)
			if err != nil {
				return nil, false, err
			}
			associatedDomains.Insert(domains...)
		}
	}
	if associatedDomains.Len() == 0 && !allScaledToZero {
		associatedReady = false
	}

	return sets.List(associatedDomains), associatedReady, nil
}

func associatedPodDomains(ctx context.Context, cl client.Client, topologyResolver topologyresolver.Resolver, pod *corev1.Pod, level grovecorev1alpha1.TopologyLevel) ([]string, error) {
	if len(pod.Spec.ResourceClaims) > 0 && len(level.ResourceSliceAttributes) > 0 {
		allocated := make([]resourcev1.DeviceRequestAllocationResult, 0)
		for _, podClaim := range pod.Spec.ResourceClaims {
			claimName, err := resourceClaimName(pod, podClaim)
			if err != nil {
				return nil, err
			}
			claim := &resourcev1.ResourceClaim{}
			if err := cl.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: claimName}, claim); err != nil {
				return nil, fmt.Errorf("failed to get ResourceClaim %q for Pod %q: %w", claimName, client.ObjectKeyFromObject(pod), err)
			}
			if claim.Status.Allocation == nil {
				return nil, fmt.Errorf("ResourceClaim %q for Pod %q is not allocated", claimName, client.ObjectKeyFromObject(pod))
			}
			allocated = append(allocated, claim.Status.Allocation.Devices.Results...)
		}
		domains, topologyBearing, err := topologyResolver.ResourceSliceDeviceDomains(ctx, level.ResourceSliceAttributes, allocated)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve allocated devices for Pod %q: %w", client.ObjectKeyFromObject(pod), err)
		}
		if topologyBearing {
			if len(domains) > 1 {
				return nil, fmt.Errorf("pod %q resolves to multiple topology domains from allocated devices: %v", client.ObjectKeyFromObject(pod), domains)
			}
			values, err := topologyResolver.NodeValues(ctx, level.Key)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve topology label %q: %w", level.Key, err)
			}
			if len(domains) == 1 && !slices.Contains(values, domains[0]) {
				return nil, fmt.Errorf("topology domain %q resolved for pod %q is not selectable by Node label %q", domains[0], client.ObjectKeyFromObject(pod), level.Key)
			}
			return domains, nil
		}
	}
	if level.Key == "" {
		return nil, fmt.Errorf("pod %q has no topology-bearing allocated devices and topology level %q has no Node label representation", client.ObjectKeyFromObject(pod), level.Domain)
	}
	value, err := topologyResolver.NodeValue(ctx, level.Key, pod.Spec.NodeName)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve topology label %q for node %q: %w", level.Key, pod.Spec.NodeName, err)
	}
	return []string{value}, nil
}

func resourceClaimName(pod *corev1.Pod, claim corev1.PodResourceClaim) (string, error) {
	if claim.ResourceClaimName != nil {
		return *claim.ResourceClaimName, nil
	}
	for _, status := range pod.Status.ResourceClaimStatuses {
		if status.Name == claim.Name && status.ResourceClaimName != nil {
			return *status.ResourceClaimName, nil
		}
	}
	return "", fmt.Errorf("pod %q ResourceClaim reference %q has no concrete claim name", client.ObjectKeyFromObject(pod), claim.Name)
}

// AssociatedPodCliqueFQNs resolves unqualified cliqueNames in topologyAffinity
// into concrete PodClique names for the target's PodCliqueScalingGroup replica.
func AssociatedPodCliqueFQNs(pcs *grovecorev1alpha1.PodCliqueSet, pclq *grovecorev1alpha1.PodClique, affinity *grovecorev1alpha1.TopologyAffinity) ([]string, error) {
	pcsgName, ok := pclq.Labels[apicommon.LabelPodCliqueScalingGroup]
	if !ok || pcsgName == "" {
		return nil, fmt.Errorf("pod clique %q in PodCliqueSet %q is missing label %q", pclq.Name, pcs.Name, apicommon.LabelPodCliqueScalingGroup)
	}
	pcsgReplicaIndexValue, ok := pclq.Labels[apicommon.LabelPodCliqueScalingGroupReplicaIndex]
	if !ok || pcsgReplicaIndexValue == "" {
		return nil, fmt.Errorf("pod clique %q in PodCliqueSet %q is missing label %q", pclq.Name, pcs.Name, apicommon.LabelPodCliqueScalingGroupReplicaIndex)
	}
	pcsgReplicaIndex, err := strconv.Atoi(pcsgReplicaIndexValue)
	if err != nil {
		return nil, fmt.Errorf("pod clique %q has invalid PodCliqueScalingGroup replica index %q: %w", pclq.Name, pcsgReplicaIndexValue, err)
	}
	if pcsgReplicaIndex < 0 {
		return nil, fmt.Errorf("pod clique %q has negative PodCliqueScalingGroup replica index %d", pclq.Name, pcsgReplicaIndex)
	}
	names := make([]string, 0, len(affinity.CliqueNames))
	for _, cliqueName := range affinity.CliqueNames {
		names = append(names, apicommon.GeneratePodCliqueName(apicommon.ResourceNameReplica{Name: pcsgName, Replica: pcsgReplicaIndex}, cliqueName))
	}
	return lo.Uniq(names), nil
}
