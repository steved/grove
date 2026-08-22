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

//go:build e2e

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ai-dynamo/grove/operator/e2e/k8s"
	"github.com/ai-dynamo/grove/operator/e2e/k8s/k8sclient"
	k8spods "github.com/ai-dynamo/grove/operator/e2e/k8s/pods"
	"github.com/ai-dynamo/grove/operator/e2e/setup"
	"github.com/ai-dynamo/grove/operator/e2e/waiter"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// groveCRDNames is the authoritative list of all CRDs that the crd-installer init container must apply.
var groveCRDNames = []string{
	"podcliques.grove.io",
	"podcliquesets.grove.io",
	"podcliquescalinggroups.grove.io",
	"clustertopologybindings.grove.io",
	"podgangmaps.grove.io",
	"podgangs.scheduler.grove.io",
}

func operatorPod(t *testing.T, ctx context.Context, k8sClient *k8sclient.Client) v1.Pod {
	t.Helper()
	var podList v1.PodList
	if err := k8sClient.List(ctx, &podList, client.InNamespace(setup.OperatorNamespace), setup.OperatorPodLabels); err != nil {
		t.Fatalf("failed to list operator pods: %v", err)
	}
	if len(podList.Items) == 0 {
		t.Fatalf("no operator pods found in namespace %s", setup.OperatorNamespace)
	}
	return podList.Items[0]
}

func ensureCRDInstallerEnabled(t *testing.T, ctx context.Context, k8sClient *k8sclient.Client) {
	t.Helper()
	for _, container := range operatorPod(t, ctx, k8sClient).Spec.InitContainers {
		if container.Name == "crd-installer" {
			return
		}
	}

	chartDir, err := setup.GetGroveChartDir()
	if err != nil {
		t.Fatalf("failed to get Grove chart directory: %v", err)
	}
	if err := setup.UpdateGroveConfiguration(ctx, k8sClient.RestConfig, chartDir, &setup.GroveConfig{InstallCRDs: true}, Logger); err != nil {
		t.Fatalf("failed to enable crd-installer: %v", err)
	}
	t.Cleanup(func() {
		if err := setup.UpdateGroveConfiguration(ctx, k8sClient.RestConfig, chartDir, &setup.GroveConfig{InstallCRDs: false}, Logger); err != nil {
			t.Fatalf("failed to restore disabled crd-installer: %v", err)
		}
	})
}

func requireCRDInstallerCompleted(t *testing.T, pod v1.Pod) {
	t.Helper()
	var crdInstallerStatus *v1.ContainerStatus
	for i := range pod.Status.InitContainerStatuses {
		if pod.Status.InitContainerStatuses[i].Name == "crd-installer" {
			crdInstallerStatus = &pod.Status.InitContainerStatuses[i]
			break
		}
	}

	if crdInstallerStatus == nil {
		t.Fatalf("crd-installer init container not found in pod %s; init containers present: %v",
			pod.Name, k8spods.InitContainerNames(pod))
	}
	if crdInstallerStatus.State.Terminated == nil {
		t.Fatalf("crd-installer init container in pod %s is not in Terminated state: %+v",
			pod.Name, crdInstallerStatus.State)
	}
	if crdInstallerStatus.State.Terminated.ExitCode != 0 {
		t.Errorf("crd-installer init container in pod %s exited with code %d (expected 0)",
			pod.Name, crdInstallerStatus.State.Terminated.ExitCode)
	}
}

// Test_CRD_Installer_AllCRDsExist verifies that all 6 Grove CRDs are present and
// established in the cluster after the operator has been deployed.
// This test does not require crdInstaller.enabled=true — CRDs are installed by the
// Helm crds/ directory on fresh install regardless of the crdInstaller flag.
func Test_CRD_Installer_AllCRDsExist(t *testing.T) {
	ctx := context.Background()
	sharedCluster := setup.SharedCluster(Logger)
	k8sClient := sharedCluster.GetClient()

	for _, crdName := range groveCRDNames {
		crd := &unstructured.Unstructured{}
		crd.SetGroupVersionKind(customResourceDefinition)
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: crdName}, crd); err != nil {
			t.Errorf("CRD %q not found: %v", crdName, err)
			continue
		}
		// Verify the CRD is established (API server is serving it).
		conditions, found, err := k8s.GetNestedSlice(crd.Object, "status", "conditions")
		if err != nil || !found {
			t.Errorf("CRD %q has no status conditions", crdName)
			continue
		}
		established := false
		for _, c := range conditions {
			cond, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			if cond["type"] == "Established" && cond["status"] == "True" {
				established = true
				break
			}
		}
		if !established {
			t.Errorf("CRD %q exists but is not Established", crdName)
		}
	}
}

// Test_CRD_Installer_InitContainerCompleted verifies that the crd-installer init container
// in the operator Pod ran to successful completion (exit code 0) when crdInstaller.enabled=true.
func Test_CRD_Installer_InitContainerCompleted(t *testing.T) {
	ctx := context.Background()
	sharedCluster := setup.SharedCluster(Logger)
	k8sClient := sharedCluster.GetClient()

	ensureCRDInstallerEnabled(t, ctx, k8sClient)
	requireCRDInstallerCompleted(t, operatorPod(t, ctx, k8sClient))
}

// Test_CRD_Installer_Idempotent verifies that restarting the operator Pod (which re-runs
// the crd-installer init container) does not corrupt or remove existing CRDs.
// crdInstaller.enabled=true is required for this test since the idempotency check relies
// on the init container running again on pod restart.
func Test_CRD_Installer_Idempotent(t *testing.T) {
	ctx := context.Background()
	sharedCluster := setup.SharedCluster(Logger)
	k8sClient := sharedCluster.GetClient()

	ensureCRDInstallerEnabled(t, ctx, k8sClient)
	podName := operatorPod(t, ctx, k8sClient).Name

	// Delete the pod to force a restart (Deployment will recreate it).
	if err := k8sClient.Delete(ctx, &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: setup.OperatorNamespace}}); err != nil {
		t.Fatalf("failed to delete operator pod %s: %v", podName, err)
	}
	Logger.Infof("deleted operator pod %s, waiting for replacement to be ready", podName)

	// Wait for the deleted pod to disappear so it cannot satisfy the readiness check below.
	w := waiter.New[*v1.Pod]().WithTimeout(3 * time.Minute).WithInterval(time.Second)
	if err := waiter.WaitForResourceDeletion(ctx, w, podName, k8sclient.Getter[*v1.Pod](k8sClient, setup.OperatorNamespace)); err != nil {
		t.Fatalf("operator pod %s was not deleted: %v", podName, err)
	}

	// Wait for a new, ready operator pod to appear.
	if err := k8spods.NewPodManager(k8sClient, Logger).WaitForReadyCount(ctx, setup.OperatorNamespace, setup.OperatorPodLabelSelector, 1, 3*time.Minute, 5*time.Second); err != nil {
		t.Fatalf("operator pod did not become ready after restart: %v", err)
	}
	requireCRDInstallerCompleted(t, operatorPod(t, ctx, k8sClient))

	// All 6 CRDs must still exist and be Established after the restart.
	for _, crdName := range groveCRDNames {
		crd := &unstructured.Unstructured{}
		crd.SetGroupVersionKind(customResourceDefinition)
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: crdName}, crd); err != nil {
			t.Errorf("CRD %q missing after operator pod restart: %v", crdName, err)
		}
	}
}
