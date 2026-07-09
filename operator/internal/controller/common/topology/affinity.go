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

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"

	"github.com/samber/lo"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LevelForDomain resolves a ClusterTopologyBinding domain and returns its generation.
func LevelForDomain(ctx context.Context, cl client.Client, topologyName string, domain grovecorev1alpha1.TopologyDomain) (grovecorev1alpha1.TopologyLevel, int64, error) {
	binding := &grovecorev1alpha1.ClusterTopologyBinding{}
	if err := cl.Get(ctx, client.ObjectKey{Name: topologyName}, binding); err != nil {
		return grovecorev1alpha1.TopologyLevel{}, 0, err
	}
	level, ok := lo.Find(binding.Spec.Levels, func(level grovecorev1alpha1.TopologyLevel) bool {
		return level.Domain == domain
	})
	if !ok {
		return grovecorev1alpha1.TopologyLevel{}, 0, fmt.Errorf("topology domain %q not found in ClusterTopologyBinding %q", domain, topologyName)
	}
	return level, binding.Generation, nil
}
