package ebs

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/kris-nova/logger"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	// Object annotations that stores information about used provisioner.
	volumeProvisionedBy = "pv.kubernetes.io/provisioned-by"
	// Possible provisioners (provided by AWS) for EBS volumes.
	awsEbsCsiDriver    = "aws-ebs-csi-driver"
	awsEbsInTreePlugin = "kubernetes.io/aws-ebs"
)

// Cleanup finds and deletes any EBS volumes associated to a Kubernetes Persistent Volumes.
func Cleanup(ctx context.Context, ec2API ec2iface.EC2API, kubernetesCS kubernetes.Interface) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("no context deadline set in call to ebs.Cleanup()")
	}

	pvs, err := kubernetesCS.CoreV1().PersistentVolumes().List(metav1.ListOptions{})
	if err != nil {
		errStr := fmt.Sprintf("cannot list Kubernetes Persistent Volumes: %s", err)
		if k8serrors.IsForbidden(err) {
			errStr = fmt.Sprintf("%s (deleting a cluster requires permission to list Kubernetes Persistent Volumes)", errStr)
		}
		return errors.New(errStr)
	}

	ebsVolumeIDs := list.New() // List of volumes IDs to track deletion.
	for _, pv := range pvs.Items {
		var volumeID string
		switch vpb := pv.Annotations[volumeProvisionedBy]; vpb {
		case awsEbsCsiDriver:
			volumeID = pv.Spec.CSI.VolumeHandle
		case awsEbsInTreePlugin:
			// Format is aws://awsRegion/volumeID thus get value after last slash.
			volumeID = pv.Spec.AWSElasticBlockStore.VolumeID
			volumeID = volumeID[strings.LastIndex(volumeID, "/")+1:]
		default:
			logger.Debug("PV %s is not EBS, it is '%s': '%s', skip", pv.Name, volumeProvisionedBy, vpb)
		}
		ebsVolumeIDs.PushBack(volumeID)
		// Disable deletion protection for volumes in use - removing all finalizers.
		pv.Finalizers = nil
		logger.Debug("updating Kubernetes Persistent Volume (settings finalizers to null) %s", pv.Name)
		if _, err := kubernetesCS.CoreV1().PersistentVolumes().Update(&pv); err != nil {
			errStr := fmt.Sprintf("cannot update Kubernetes Persistent Volume %s: %s", pv.Name, err)
			if k8serrors.IsForbidden(err) {
				errStr = fmt.Sprintf("%s (deleting a cluster requires permission to patch Kubernetes Persistent Volume)", errStr)
			}
			return errors.New(errStr)
		}
		logger.Debug("deleting Kubernetes Persistent Volume %s", pv.Name)
		if err := kubernetesCS.CoreV1().PersistentVolumes().Delete(pv.Name, &metav1.DeleteOptions{}); err != nil {
			errStr := fmt.Sprintf("cannot delete Kubernetes Persistent Volume %s: %s", pv.Name, err)
			if k8serrors.IsForbidden(err) {
				errStr = fmt.Sprintf("%s (deleting a cluster requires permission to delete Kubernetes Persistent Volume)", errStr)
			}
			return errors.New(errStr)
		}
	}

	// Wait for all the EBS volumes backing the Kubernetes Persistent Volumes to disappear.
	pollInterval := 2 * time.Second
	for ; time.Now().Before(deadline) && ebsVolumeIDs.Len() > 0; time.Sleep(pollInterval) {
		var ebsNext *list.Element
		for ebs := ebsVolumeIDs.Front(); ebs != nil; ebs = ebsNext {
			ebsNext = ebs.Next()
			ebsID := ebs.Value.(string)
			exists, err := ebsExists(ctx, ec2API, ebsID)
			if err != nil {
				logger.Warning("error when checking existence of AWS Elastic Block Storage %s: %s", ebsID, err)
				continue
			}
			if !exists {
				// The EBS has been deleted.
				logger.Debug("AWS Elastic Block Storage %s was deleted", ebsID)
				ebsVolumeIDs.Remove(ebs)
			}
		}
	}
	if numEbs := ebsVolumeIDs.Len(); numEbs > 0 {
		ebsIDs := make([]string, 0, numEbs)
		for ebs := ebsVolumeIDs.Front(); ebs != nil; ebs = ebs.Next() {
			ebsIDs = append(ebsIDs, ebs.Value.(string))
		}
		return fmt.Errorf("deadline surpassed waiting for AWS Elastic Block Storages to be deleted: %s", strings.Join(ebsIDs, ", "))
	}
	return nil
}

func ebsExists(ctx context.Context, ec2API ec2iface.EC2API, ebsID string) (bool, error) {
	result, err := ec2API.DescribeVolumesWithContext(ctx, &ec2.DescribeVolumesInput{VolumeIds: []*string{aws.String(ebsID)}})
	if err != nil {
		if awsError, ok := err.(awserr.Error); ok && awsError.Code() == "InvalidVolume.NotFound" {
			return false, nil
		}
		return false, err
	}
	if len(result.Volumes) > 0 && aws.StringValue(result.Volumes[0].State) == ec2.VolumeStateDeleted {
		return false, nil
	}
	return true, nil
}
