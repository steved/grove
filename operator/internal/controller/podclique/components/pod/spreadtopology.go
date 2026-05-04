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

package pod

import (
	"fmt"
	"os"
	"strconv"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"

	corev1 "k8s.io/api/core/v1"
)

const (
	defaultTopologySpreadPlaceholderImage            = "registry.k8s.io/pause:latest"
	envVarTopologySpreadPlaceholderImage             = "GROVE_SPREAD_PLACEHOLDER_IMAGE"
	envVarTopologySpreadPlaceholderPriorityClassName = "GROVE_SPREAD_PLACEHOLDER_PRIORITY_CLASS_NAME"
)

func configureSpreadTopologyPod(pclqAnnotations map[string]string, pclqName string, pod *corev1.Pod, podIndex int) error {
	phase := componentutils.GetTopologySpreadPhase(pclqAnnotations)
	if phase == "" {
		return nil
	}
	domain, ok := componentutils.TopologySpreadDomainForPodIndexFromAnnotations(pclqAnnotations, podIndex)
	if !ok {
		return fmt.Errorf("failed to resolve spread topology domain for PodClique %q pod index %d", pclqName, podIndex)
	}
	topologyKey := componentutils.TopologySpreadTopologyKey(pclqAnnotations)
	if topologyKey == "" {
		return fmt.Errorf("missing spread topology key for PodClique %q", pclqName)
	}
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	placeholder := phase == componentutils.TopologySpreadPhasePlaceholder
	pod.Labels[apicommon.LabelTopologySpreadPlaceholder] = strconv.FormatBool(placeholder)
	pod.Labels[apicommon.LabelTopologySpreadDomain] = domain
	addRequiredNodeAffinity(&pod.Spec, topologyKey, domain)
	if placeholder {
		configureTopologySpreadPlaceholderPodSpec(&pod.Spec)
	}
	return nil
}

func configureTopologySpreadPlaceholderPodSpec(podSpec *corev1.PodSpec) {
	podSpec.InitContainers = nil
	podSpec.Containers = buildPlaceholderContainers(podSpec.Containers)
	podSpec.ReadinessGates = nil
	if priorityClassName := os.Getenv(envVarTopologySpreadPlaceholderPriorityClassName); priorityClassName != "" {
		podSpec.PriorityClassName = priorityClassName
	}
}

func buildPlaceholderContainers(containers []corev1.Container) []corev1.Container {
	image := topologySpreadPlaceholderImage()
	if len(containers) == 0 {
		return []corev1.Container{{Name: "pause", Image: image, ImagePullPolicy: corev1.PullIfNotPresent}}
	}
	placeholderContainers := make([]corev1.Container, len(containers))
	for i, container := range containers {
		placeholderContainers[i] = corev1.Container{
			Name:            container.Name,
			Image:           image,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Resources:       container.Resources,
			SecurityContext: container.SecurityContext,
		}
	}
	return placeholderContainers
}

func topologySpreadPlaceholderImage() string {
	if image := os.Getenv(envVarTopologySpreadPlaceholderImage); image != "" {
		return image
	}
	return defaultTopologySpreadPlaceholderImage
}

func addRequiredNodeAffinity(podSpec *corev1.PodSpec, topologyKey, domain string) {
	expression := corev1.NodeSelectorRequirement{
		Key:      topologyKey,
		Operator: corev1.NodeSelectorOpIn,
		Values:   []string{domain},
	}
	if podSpec.Affinity == nil {
		podSpec.Affinity = &corev1.Affinity{}
	}
	if podSpec.Affinity.NodeAffinity == nil {
		podSpec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	required := podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if required == nil || len(required.NodeSelectorTerms) == 0 {
		podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{expression}}},
		}
		return
	}
	for i := range required.NodeSelectorTerms {
		required.NodeSelectorTerms[i].MatchExpressions = append(required.NodeSelectorTerms[i].MatchExpressions, expression)
	}
}
