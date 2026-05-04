// /*
// Copyright 2025 The Grove Authors.
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

package utils

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	apicommonconstants "github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// TopologySpreadPhasePlaceholder means the PodClique should create reservation placeholder Pods.
	TopologySpreadPhasePlaceholder = "placeholder"
	// TopologySpreadPhaseActual means the PodClique should create real workload Pods in dependency-selected domains.
	TopologySpreadPhaseActual = "actual"
)

// IsSpreadTopologyConstraint reports whether the topology constraint enables spread topology.
func IsSpreadTopologyConstraint(tc *grovecorev1alpha1.TopologyConstraint) bool {
	return tc != nil && tc.Spread
}

// IsSpreadPodCliqueTemplate reports whether the PodClique template enables spread topology.
func IsSpreadPodCliqueTemplate(pclqTemplateSpec *grovecorev1alpha1.PodCliqueTemplateSpec) bool {
	return pclqTemplateSpec != nil && IsSpreadTopologyConstraint(pclqTemplateSpec.TopologyConstraint)
}

// GetTopologyKeyForDomain resolves a ClusterTopology domain to the node label key used by that domain.
func GetTopologyKeyForDomain(levels []grovecorev1alpha1.TopologyLevel, domain grovecorev1alpha1.TopologyDomain) (string, bool) {
	level, ok := lo.Find(levels, func(level grovecorev1alpha1.TopologyLevel) bool {
		return level.Domain == domain
	})
	return level.Key, ok
}

// ListTopologyDomainValues returns the unique values currently present on Nodes for the given topology label key.
func ListTopologyDomainValues(ctx context.Context, cl client.Client, topologyKey string) ([]string, error) {
	if topologyKey == "" {
		return nil, fmt.Errorf("topology key must not be empty")
	}
	nodeList := &corev1.NodeList{}
	if err := cl.List(ctx, nodeList); err != nil {
		return nil, err
	}
	values := sets.New[string]()
	for _, node := range nodeList.Items {
		if value := node.Labels[topologyKey]; value != "" {
			values.Insert(value)
		}
	}
	result := values.UnsortedList()
	slices.Sort(result)
	return result, nil
}

// ReadyDependencyTopologyDomains returns the topology domains occupied by ready dependency Pods.
func ReadyDependencyTopologyDomains(ctx context.Context, cl client.Client, namespace string, dependencyNames []string, topologyKey string, requireAllReplicas bool) ([]string, bool, error) {
	if len(dependencyNames) == 0 {
		return nil, false, nil
	}
	nodesByName, err := listNodesByName(ctx, cl)
	if err != nil {
		return nil, false, err
	}
	domains := sets.New[string]()
	for _, dependencyName := range dependencyNames {
		parent := &grovecorev1alpha1.PodClique{}
		parentKey := client.ObjectKey{Namespace: namespace, Name: dependencyName}
		if err := cl.Get(ctx, parentKey, parent); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, false, nil
			}
			return nil, false, err
		}
		required := int(lo.FromPtr(parent.Spec.MinAvailable))
		if requireAllReplicas {
			required = int(parent.Spec.Replicas)
		}
		if required == 0 {
			return nil, false, nil
		}
		readyInDependency := 0
		parentPods, err := GetPCLQPods(ctx, cl, "", parent)
		if err != nil {
			return nil, false, err
		}
		for _, pod := range parentPods {
			if isTopologySpreadPlaceholderPod(pod) || !isPodScheduledAndReady(pod) {
				continue
			}
			node := nodesByName[pod.Spec.NodeName]
			if node == nil {
				continue
			}
			if domainValue := node.Labels[topologyKey]; domainValue != "" {
				readyInDependency++
				domains.Insert(domainValue)
			}
		}
		if readyInDependency < required {
			return nil, false, nil
		}
	}
	result := domains.UnsortedList()
	slices.Sort(result)
	return result, len(result) > 0, nil
}

func listNodesByName(ctx context.Context, cl client.Client) (map[string]*corev1.Node, error) {
	nodeList := &corev1.NodeList{}
	if err := cl.List(ctx, nodeList); err != nil {
		return nil, err
	}
	nodesByName := make(map[string]*corev1.Node, len(nodeList.Items))
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		nodesByName[node.Name] = node
	}
	return nodesByName, nil
}

func isPodScheduledAndReady(pod *corev1.Pod) bool {
	if pod.Spec.NodeName == "" {
		return false
	}
	return isPodConditionTrue(pod, corev1.PodScheduled) && isPodConditionTrue(pod, corev1.PodReady)
}

func isPodConditionTrue(pod *corev1.Pod, conditionType corev1.PodConditionType) bool {
	return slices.ContainsFunc(pod.Status.Conditions, func(condition corev1.PodCondition) bool {
		return condition.Type == conditionType && condition.Status == corev1.ConditionTrue
	})
}

func isTopologySpreadPlaceholderPod(pod *corev1.Pod) bool {
	return pod.Labels[apicommon.LabelTopologySpreadPlaceholder] == "true"
}

// IsTopologySpreadPlaceholderPod reports whether the Pod is a spread topology placeholder.
func IsTopologySpreadPlaceholderPod(pod *corev1.Pod) bool {
	return isTopologySpreadPlaceholderPod(pod)
}

// IsTopologySpreadPodInDesiredActualSlot reports whether a spread Pod is an actual Pod whose assigned
// domain matches the current desired domain for its PodClique index.
func IsTopologySpreadPodInDesiredActualSlot(pclq *grovecorev1alpha1.PodClique, pod *corev1.Pod) bool {
	if GetTopologySpreadPhase(pclq.Annotations) != TopologySpreadPhaseActual {
		return false
	}
	if IsTopologySpreadPlaceholderPod(pod) {
		return false
	}
	podIndex, err := strconv.Atoi(pod.Labels[apicommon.LabelPodCliquePodIndex])
	if err != nil {
		return false
	}
	expectedDomain, ok := TopologySpreadDomainForPodIndex(pclq, podIndex)
	if !ok {
		return false
	}
	return pod.Labels[apicommon.LabelTopologySpreadDomain] == expectedDomain
}

// SetTopologySpreadAnnotations stores spread phase metadata on a PodClique.
func SetTopologySpreadAnnotations(pclq *grovecorev1alpha1.PodClique, phase, topologyKey string, allDomains, activeDomains []string, replicasPerDomain, minAvailablePerDomain int32) {
	if pclq.Annotations == nil {
		pclq.Annotations = map[string]string{}
	}
	pclq.Annotations[apicommonconstants.AnnotationTopologySpreadPhase] = phase
	pclq.Annotations[apicommonconstants.AnnotationTopologySpreadTopologyKey] = topologyKey
	pclq.Annotations[apicommonconstants.AnnotationTopologySpreadAllDomains] = strings.Join(allDomains, ",")
	pclq.Annotations[apicommonconstants.AnnotationTopologySpreadActiveDomains] = strings.Join(activeDomains, ",")
	pclq.Annotations[apicommonconstants.AnnotationTopologySpreadReplicasPerDomain] = strconv.Itoa(int(replicasPerDomain))
	pclq.Annotations[apicommonconstants.AnnotationTopologySpreadMinAvailablePerDomain] = strconv.Itoa(int(minAvailablePerDomain))
}

// GetTopologySpreadPhase returns the spread phase stored on an object.
func GetTopologySpreadPhase(annotations map[string]string) string {
	return annotations[apicommonconstants.AnnotationTopologySpreadPhase]
}

// IsTopologySpreadPodClique reports whether a PodClique is currently managed by spread topology.
func IsTopologySpreadPodClique(pclq *grovecorev1alpha1.PodClique) bool {
	phase := GetTopologySpreadPhase(pclq.Annotations)
	return phase == TopologySpreadPhasePlaceholder || phase == TopologySpreadPhaseActual
}

// TopologySpreadDomainsForPhase returns the ordered domains used by a spread PodClique phase.
func TopologySpreadDomainsForPhase(annotations map[string]string, phase string) []string {
	switch phase {
	case TopologySpreadPhasePlaceholder:
		return splitTopologySpreadDomains(annotations[apicommonconstants.AnnotationTopologySpreadAllDomains])
	case TopologySpreadPhaseActual:
		return splitTopologySpreadDomains(annotations[apicommonconstants.AnnotationTopologySpreadActiveDomains])
	default:
		return nil
	}
}

func splitTopologySpreadDomains(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	domains := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			domains = append(domains, part)
		}
	}
	return domains
}

// TopologySpreadReplicasPerDomain returns the per-domain replica count stored on a spread PodClique.
func TopologySpreadReplicasPerDomain(annotations map[string]string) int {
	value, err := strconv.Atoi(annotations[apicommonconstants.AnnotationTopologySpreadReplicasPerDomain])
	if err != nil || value <= 0 {
		return 0
	}
	return value
}

// TopologySpreadTopologyKey returns the node label key backing a spread PodClique.
func TopologySpreadTopologyKey(annotations map[string]string) string {
	return annotations[apicommonconstants.AnnotationTopologySpreadTopologyKey]
}

// TopologySpreadDomainForPodIndex maps a Pod index to its assigned topology domain.
func TopologySpreadDomainForPodIndex(pclq *grovecorev1alpha1.PodClique, podIndex int) (string, bool) {
	return TopologySpreadDomainForPodIndexFromAnnotations(pclq.Annotations, podIndex)
}

// TopologySpreadDomainForPodIndexFromAnnotations maps a Pod index to its assigned topology domain.
func TopologySpreadDomainForPodIndexFromAnnotations(annotations map[string]string, podIndex int) (string, bool) {
	phase := GetTopologySpreadPhase(annotations)
	domains := TopologySpreadDomainsForPhase(annotations, phase)
	replicasPerDomain := TopologySpreadReplicasPerDomain(annotations)
	if phase == "" || len(domains) == 0 || replicasPerDomain <= 0 {
		return "", false
	}
	domainIndex := podIndex / replicasPerDomain
	if domainIndex < 0 || domainIndex >= len(domains) {
		return "", false
	}
	return domains[domainIndex], true
}

// TopologySpreadSelectorForActualPods returns labels that select actual spread Pods for a PodClique.
func TopologySpreadSelectorForActualPods(pclq *grovecorev1alpha1.PodClique) labels.Set {
	return labels.Set{
		apicommon.LabelPodClique:                 pclq.Name,
		apicommon.LabelTopologySpreadPlaceholder: "false",
	}
}
