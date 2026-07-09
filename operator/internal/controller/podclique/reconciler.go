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

	"github.com/ai-dynamo/grove/operator/api/common/constants"
	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	ctrlcommon "github.com/ai-dynamo/grove/operator/internal/controller/common"
	"github.com/ai-dynamo/grove/operator/internal/controller/common/component"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"
	pclqcomponent "github.com/ai-dynamo/grove/operator/internal/controller/podclique/components"
	"github.com/ai-dynamo/grove/operator/internal/controller/topologyresolver"
	ctrlutils "github.com/ai-dynamo/grove/operator/internal/controller/utils"
	"github.com/ai-dynamo/grove/operator/internal/expect"
	"github.com/ai-dynamo/grove/operator/internal/scheduler"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	ctrllogger "sigs.k8s.io/controller-runtime/pkg/log"
)

// Reconciler reconciles PodClique objects.
type Reconciler struct {
	config                  configv1alpha1.PodCliqueControllerConfiguration
	client                  ctrlclient.Client
	eventRecorder           record.EventRecorder
	reconcileStatusRecorder ctrlcommon.ReconcileErrorRecorder
	expectationsStore       *expect.ExpectationsStore
	operatorRegistry        component.OperatorRegistry[grovecorev1alpha1.PodClique]
	topologyResolver        topologyresolver.Resolver
	topologyAffinityEnabled bool
}

// NewReconciler creates a new instance of the PodClique Reconciler.
func NewReconciler(mgr ctrl.Manager, controllerCfg configv1alpha1.PodCliqueControllerConfiguration, schedRegistry scheduler.Registry, topologyResolver topologyresolver.Resolver, topologyAffinityEnabled bool) *Reconciler {
	eventRecorder := mgr.GetEventRecorderFor(controllerName)
	expectationsStore := expect.NewExpectationsStore()
	return &Reconciler{
		config:                  controllerCfg,
		client:                  mgr.GetClient(),
		eventRecorder:           eventRecorder,
		reconcileStatusRecorder: ctrlcommon.NewReconcileErrorRecorder(mgr.GetClient()),
		expectationsStore:       expectationsStore,
		operatorRegistry:        pclqcomponent.CreateOperatorRegistry(mgr, eventRecorder, expectationsStore, schedRegistry, topologyResolver),
		topologyResolver:        topologyResolver,
		topologyAffinityEnabled: topologyAffinityEnabled,
	}
}

// Reconcile reconciles the `PodClique` resource.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrllogger.FromContext(ctx).WithName(controllerName)

	// Memoize lookups that happen multiple times within a single reconcile:
	//   * GetPCLQPods — reconcileSpec + reconcileStatus each list pods
	//   * GetPodCliqueSet — called 4× (spec, status, pod sync, resourceclaim)
	ctx = componentutils.WithPCLQPodsCache(ctx)
	ctx = componentutils.WithPodCliqueSetCache(ctx)

	pclq := &grovecorev1alpha1.PodClique{}
	if result := ctrlutils.GetPodClique(ctx, r.client, logger, req.NamespacedName, pclq, true); ctrlcommon.ShortCircuitReconcileFlow(result) {
		return result.Result()
	}

	if !pclq.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(pclq, constants.FinalizerPodClique) {
			return ctrlcommon.DoNotRequeue().Result()
		}
		return r.triggerDeletionFlow(ctx, logger, pclq).Result()
	}
	if !r.topologyAffinityEnabled && pclq.Spec.Affinity != nil && pclq.Spec.Affinity.TopologyAffinity != nil {
		if err := r.markTopologyAffinityDisabled(ctx, pclq); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	reconcileSpecFlowResult := r.reconcileSpec(ctx, logger, pclq)
	if statusReconcileResult := r.reconcileStatus(ctx, logger, pclq); ctrlcommon.ShortCircuitReconcileFlow(statusReconcileResult) {
		return statusReconcileResult.Result()
	}

	return reconcileSpecFlowResult.Result()
}

func (r *Reconciler) markTopologyAffinityDisabled(ctx context.Context, pclq *grovecorev1alpha1.PodClique) error {
	return r.patchTopologyAffinityFailure(ctx, pclq, constants.ConditionReasonFeatureDisabled, "PodCliqueTopologyAffinity feature gate is disabled", true)
}

func (r *Reconciler) patchTopologyAffinityFailure(ctx context.Context, pclq *grovecorev1alpha1.PodClique, reason, message string, suppressGangTermination bool) error {
	patch := ctrlclient.MergeFrom(pclq.DeepCopy())
	originalStatus := pclq.Status.DeepCopy()
	now := metav1.Now()
	meta.SetStatusCondition(&pclq.Status.Conditions, metav1.Condition{
		Type:               constants.ConditionTopologyAffinityReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: pclq.Generation,
		LastTransitionTime: now,
	})
	if suppressGangTermination {
		meta.SetStatusCondition(&pclq.Status.Conditions, metav1.Condition{
			Type:               constants.ConditionTypeMinAvailableBreached,
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            "Gang termination is suppressed while PodCliqueTopologyAffinity is disabled",
			ObservedGeneration: pclq.Generation,
			LastTransitionTime: now,
		})
	}
	if equality.Semantic.DeepEqual(*originalStatus, pclq.Status) {
		return nil
	}
	if err := r.client.Status().Patch(ctx, pclq, patch); err != nil {
		return fmt.Errorf("failed to mark topology affinity failure for PodClique %s: %w", ctrlclient.ObjectKeyFromObject(pclq), err)
	}
	return nil
}
