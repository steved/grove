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

package podclique

import (
	"context"
	"fmt"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/clustertopology"
	"github.com/ai-dynamo/grove/operator/internal/controller/common/component"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"
	groveerr "github.com/ai-dynamo/grove/operator/internal/errors"

	"github.com/go-logr/logr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	errCodeApplySpreadTopology grovecorev1alpha1.ErrorCode = "ERR_APPLY_SPREAD_TOPOLOGY"
)

func (r _resource) applySpreadTopologyState(ctx context.Context, logger logr.Logger, pcs *grovecorev1alpha1.PodCliqueSet, pclq *grovecorev1alpha1.PodClique, pclqTemplateSpec *grovecorev1alpha1.PodCliqueTemplateSpec, dependencyNames []string) error {
	if !componentutils.IsSpreadPodCliqueTemplate(pclqTemplateSpec) {
		return nil
	}

	tc := pclqTemplateSpec.TopologyConstraint
	topologyLevels, err := clustertopology.GetClusterTopologyLevels(ctx, r.client, tc.TopologyName)
	if err != nil {
		return groveerr.WrapError(err,
			errCodeApplySpreadTopology,
			component.OperationSync,
			fmt.Sprintf("failed to get ClusterTopology %q for spread PodClique %v", tc.TopologyName, client.ObjectKeyFromObject(pclq)),
		)
	}
	topologyKey, ok := componentutils.GetTopologyKeyForDomain(topologyLevels, tc.PackDomain)
	if !ok {
		return groveerr.New(
			errCodeApplySpreadTopology,
			component.OperationSync,
			fmt.Sprintf("ClusterTopology %q does not define spread packDomain %q for PodClique %v", tc.TopologyName, tc.PackDomain, client.ObjectKeyFromObject(pclq)),
		)
	}

	allDomains, err := componentutils.ListTopologyDomainValues(ctx, r.client, topologyKey)
	if err != nil {
		return groveerr.WrapError(err,
			errCodeApplySpreadTopology,
			component.OperationSync,
			fmt.Sprintf("failed to list Nodes for spread PodClique %v", client.ObjectKeyFromObject(pclq)),
		)
	}
	if len(allDomains) == 0 {
		return groveerr.New(
			errCodeApplySpreadTopology,
			component.OperationSync,
			fmt.Sprintf("no Node has topology key %q for spread PodClique %v", topologyKey, client.ObjectKeyFromObject(pclq)),
		)
	}

	phase := componentutils.TopologySpreadPhasePlaceholder
	activeDomains := []string{}
	selectedDomains := allDomains
	readyDomains, ready, err := componentutils.ReadyDependencyTopologyDomains(ctx, r.client, pcs.Namespace, dependencyNames, topologyKey, false)
	if err != nil {
		return groveerr.WrapError(err,
			errCodeApplySpreadTopology,
			component.OperationSync,
			fmt.Sprintf("failed to resolve ready dependency topology domains for spread PodClique %v", client.ObjectKeyFromObject(pclq)),
		)
	}
	if ready {
		phase = componentutils.TopologySpreadPhaseActual
		activeDomains = readyDomains
		selectedDomains = readyDomains
	}

	replicasPerDomain := pclqTemplateSpec.Spec.Replicas
	minAvailablePerDomain := ptr.Deref(pclqTemplateSpec.Spec.MinAvailable, replicasPerDomain)
	pclq.Spec.Replicas = int32(len(selectedDomains)) * replicasPerDomain
	minAvailable := int32(len(selectedDomains)) * minAvailablePerDomain
	pclq.Spec.MinAvailable = &minAvailable
	componentutils.SetTopologySpreadAnnotations(pclq, phase, topologyKey, allDomains, activeDomains, replicasPerDomain, minAvailablePerDomain)

	logger.Info("Applied spread topology state to PodClique",
		"podClique", client.ObjectKeyFromObject(pclq),
		"phase", phase,
		"topologyKey", topologyKey,
		"allDomains", allDomains,
		"activeDomains", activeDomains,
		"replicas", pclq.Spec.Replicas,
		"minAvailable", minAvailable,
	)
	return nil
}
