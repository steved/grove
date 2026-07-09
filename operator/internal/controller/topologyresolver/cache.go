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

package topologyresolver

import (
	"context"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"

	resourcev1 "k8s.io/api/resource/v1"
)

// Resolver provides the shared topology views used by PodClique reconciliation.
type Resolver interface {
	NodeValues(ctx context.Context, labelKey string) ([]string, error)
	NodeValue(ctx context.Context, labelKey, nodeName string) (string, error)
	ResourceSliceDeviceDomains(ctx context.Context, refs []grovecorev1alpha1.ResourceSliceAttributeReference, devices []resourcev1.DeviceRequestAllocationResult) ([]string, bool, error)
}

var _ Resolver = (*Reconciler)(nil)
