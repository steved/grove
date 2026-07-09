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

package validation

import (
	"context"
	"fmt"
	"strings"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/clustertopology"

	"github.com/samber/lo"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const scaleSubResource = "scale"

func (v *pcsValidator) validatePodCliqueTopologyAffinity(clique *grovecorev1alpha1.PodCliqueTemplateSpec, fldPath *field.Path) field.ErrorList {
	if clique.Spec.Affinity == nil || clique.Spec.Affinity.TopologyAffinity == nil {
		return nil
	}

	allErrs := field.ErrorList{}
	affinity := clique.Spec.Affinity.TopologyAffinity
	affinityPath := fldPath.Child("spec", "affinity", "topologyAffinity")

	if affinity.TopologyName == "" {
		allErrs = append(allErrs, field.Required(affinityPath.Child("topologyName"), "topologyName is required when topologyAffinity is set"))
	}
	if affinity.Domain == "" {
		allErrs = append(allErrs, field.Required(affinityPath.Child("domain"), "domain is required when topologyAffinity is set"))
	}
	if len(affinity.CliqueNames) == 0 {
		allErrs = append(allErrs, field.Required(affinityPath.Child("cliqueNames"), "at least one associated PodClique name is required"))
	}
	allErrs = append(allErrs, sliceMustHaveUniqueElements(affinity.CliqueNames, affinityPath.Child("cliqueNames"))...)

	allCliqueNames := lo.Map(v.pcs.Spec.Template.Cliques, func(clique *grovecorev1alpha1.PodCliqueTemplateSpec, _ int) string {
		return clique.Name
	})
	for i, cliqueName := range affinity.CliqueNames {
		if !slicesContains(allCliqueNames, cliqueName) {
			allErrs = append(allErrs, field.Invalid(affinityPath.Child("cliqueNames").Index(i), cliqueName, "associated PodClique name must be defined in spec.template.cliques"))
		}
		if cliqueName == clique.Name {
			allErrs = append(allErrs, field.Invalid(affinityPath.Child("cliqueNames").Index(i), cliqueName, "topologyAffinity cannot refer to its own PodClique"))
		}
	}
	groupName := ""
	for _, group := range v.pcs.Spec.Template.PodCliqueScalingGroupConfigs {
		if slicesContains(group.CliqueNames, clique.Name) {
			groupName = group.Name
			break
		}
	}
	if groupName == "" {
		allErrs = append(allErrs, field.Invalid(affinityPath, affinity, "topologyAffinity PodClique must belong to a PodCliqueScalingGroup"))
	} else {
		for i, cliqueName := range affinity.CliqueNames {
			inSameGroup := lo.SomeBy(v.pcs.Spec.Template.PodCliqueScalingGroupConfigs, func(group grovecorev1alpha1.PodCliqueScalingGroupConfig) bool {
				return group.Name == groupName && slicesContains(group.CliqueNames, cliqueName)
			})
			if !inSameGroup {
				allErrs = append(allErrs, field.Invalid(affinityPath.Child("cliqueNames").Index(i), cliqueName, "associated PodClique must belong to the same PodCliqueScalingGroup"))
			}
		}
	}

	if clique.Spec.MinAvailable != nil && *clique.Spec.MinAvailable != clique.Spec.Replicas && !v.isScaleUpdateAboveMinAvailable(clique) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("spec", "minAvailable"), *clique.Spec.MinAvailable, "minAvailable must equal replicas when topologyAffinity is set"))
	}

	if affinity.TopologyName == "" || affinity.Domain == "" {
		return allErrs
	}
	level, errs := v.resolveTopologyAffinityLevel(context.Background(), affinity, affinityPath)
	if len(errs) > 0 {
		return append(allErrs, errs...)
	}
	allErrs = append(allErrs, validateTopologyAffinityNodeSelectorConflict(clique.Spec.PodSpec, level.Key, fldPath.Child("spec", "podSpec"))...)
	return allErrs
}

func (v *pcsValidator) isScaleUpdateAboveMinAvailable(clique *grovecorev1alpha1.PodCliqueTemplateSpec) bool {
	return v.operation == admissionv1.Update &&
		v.subResource == scaleSubResource &&
		clique.Spec.MinAvailable != nil &&
		*clique.Spec.MinAvailable < clique.Spec.Replicas
}

func (v *pcsValidator) resolveTopologyAffinityLevel(ctx context.Context, affinity *grovecorev1alpha1.TopologyAffinity, fldPath *field.Path) (grovecorev1alpha1.TopologyLevel, field.ErrorList) {
	levels, err := clustertopology.GetClusterTopologyLevels(ctx, v.client, affinity.TopologyName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return grovecorev1alpha1.TopologyLevel{}, field.ErrorList{field.Invalid(fldPath.Child("topologyName"), affinity.TopologyName,
				fmt.Sprintf("ClusterTopologyBinding %q not found", affinity.TopologyName))}
		}
		return grovecorev1alpha1.TopologyLevel{}, field.ErrorList{field.InternalError(fldPath.Child("topologyName"),
			fmt.Errorf("failed to fetch ClusterTopologyBinding %q: %w", affinity.TopologyName, err))}
	}
	level, ok := lo.Find(levels, func(level grovecorev1alpha1.TopologyLevel) bool {
		return level.Domain == affinity.Domain
	})
	if !ok {
		domains := lo.Map(levels, func(level grovecorev1alpha1.TopologyLevel, _ int) string {
			return string(level.Domain)
		})
		return grovecorev1alpha1.TopologyLevel{}, field.ErrorList{field.Invalid(fldPath.Child("domain"), affinity.Domain,
			fmt.Sprintf("topology domain %q does not exist in ClusterTopologyBinding %q; valid domains: %s", affinity.Domain, affinity.TopologyName, strings.Join(domains, ", ")))}
	}
	return level, nil
}

func validateTopologyAffinityNodeSelectorConflict(podSpec corev1.PodSpec, labelKey string, fldPath *field.Path) field.ErrorList {
	if labelKey == "" {
		return nil
	}
	allErrs := field.ErrorList{}
	if _, ok := podSpec.NodeSelector[labelKey]; ok {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("nodeSelector"), labelKey, "nodeSelector conflicts with topologyAffinity on the same node label key"))
	}
	if podSpec.Affinity == nil || podSpec.Affinity.NodeAffinity == nil {
		return allErrs
	}
	nodeAffinity := podSpec.Affinity.NodeAffinity
	if nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		for termIndex, term := range nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
			for exprIndex, expr := range term.MatchExpressions {
				if expr.Key == labelKey {
					allErrs = append(allErrs, field.Invalid(
						fldPath.Child("affinity", "nodeAffinity", "requiredDuringSchedulingIgnoredDuringExecution", "nodeSelectorTerms").Index(termIndex).Child("matchExpressions").Index(exprIndex).Child("key"),
						expr.Key,
						"nodeAffinity conflicts with topologyAffinity on the same node label key"))
				}
			}
		}
	}
	for prefIndex, pref := range nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
		for exprIndex, expr := range pref.Preference.MatchExpressions {
			if expr.Key == labelKey {
				allErrs = append(allErrs, field.Invalid(
					fldPath.Child("affinity", "nodeAffinity", "preferredDuringSchedulingIgnoredDuringExecution").Index(prefIndex).Child("preference", "matchExpressions").Index(exprIndex).Child("key"),
					expr.Key,
					"nodeAffinity conflicts with topologyAffinity on the same node label key"))
			}
		}
	}
	return allErrs
}

func validateTopologyAffinityDependencies(cliques []*grovecorev1alpha1.PodCliqueTemplateSpec, fldPath *field.Path) field.ErrorList {
	depG := NewPodCliqueDependencyGraph()
	var discoveredCliqueNames []string
	var topologyName string
	allErrs := field.ErrorList{}
	for i, clique := range cliques {
		discoveredCliqueNames = append(discoveredCliqueNames, clique.Name)
		if clique.Spec.Affinity == nil || clique.Spec.Affinity.TopologyAffinity == nil {
			continue
		}
		affinity := clique.Spec.Affinity.TopologyAffinity
		depG.AddDependencies(clique.Name, affinity.CliqueNames)
		if topologyName == "" {
			topologyName = affinity.TopologyName
		} else if affinity.TopologyName != topologyName {
			allErrs = append(allErrs, field.Invalid(fldPath.Index(i).Child("spec", "affinity", "topologyAffinity", "topologyName"), affinity.TopologyName, "all topologyAffinity rules must use the same ClusterTopologyBinding"))
		}
	}

	unknownCliqueNames := depG.GetUnknownCliques(discoveredCliqueNames)
	if len(unknownCliqueNames) > 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("spec", "affinity", "topologyAffinity", "cliqueNames"),
			strings.Join(unknownCliqueNames, ","), "unknown PodClique names found, all topologyAffinity cliqueNames must be defined as cliques"))
	}
	cycles := depG.GetStronglyConnectedCliques()
	if len(cycles) > 0 {
		allErrs = append(allErrs, field.Invalid(fldPath, cycles, "topologyAffinity must not have circular dependencies"))
	}
	return allErrs
}

func slicesContains(items []string, item string) bool {
	return sets.New(items...).Has(item)
}
