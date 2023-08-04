package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	"github.com/gocarina/gocsv"
)

type PodLabelsInfo struct {
	PodName             string
	ContainerName       string
	PodNamespace        string
	App                 string
	NextflowRunName     string
	NextflowProcessName string
	NextflowSessionID   string
	NextflowTaskName    string
}

type RunningContainer struct {
	Pid       int
	Created   string
	StartedAt string
	Image     string
	Info      PodLabelsInfo
}

var RESULT_PATH = ""

type RunningContainers []RunningContainer

func parseContainerLabels(labels map[string]string) PodLabelsInfo {
	var podLabels PodLabelsInfo
	podLabels.PodName = labels["io.kubernetes.pod.name"]
	podLabels.ContainerName = labels["io.kubernetes.container.name"]
	podLabels.PodNamespace = labels["io.kubernetes.pod.namespace"]
	podLabels.App = labels["nextflow.io/app"]
	podLabels.NextflowRunName = labels["nextflow.io/runName"]
	podLabels.NextflowProcessName = labels["nextflow.io/processName"]
	podLabels.NextflowSessionID = labels["nextflow.io/sessionId"]
	podLabels.NextflowTaskName = labels["nextflow.io/taskName"]
	return podLabels
}

func contains(l []*RunningContainer, c RunningContainer) bool {
	for _, con := range l {
		if con.Pid == c.Pid && con.Info.PodName == c.Info.PodName && con.Info.ContainerName == c.Info.ContainerName {
			return true
		}
	}
	return false
}

func init() {
	RESULT_PATH = os.Getenv("RESULT_PATH")
}

func main() {
	resultFile := RESULT_PATH + "/pid_pods.csv"
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		panic(err)
	}

	runningContainers := []*RunningContainer{}
	// check if a result file already exists
	if _, err := os.Stat(resultFile); err == nil {
		oldResults, err := os.OpenFile(resultFile, os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			panic(err)
		}
		defer oldResults.Close()

		if err := gocsv.UnmarshalFile(oldResults, &runningContainers); err != nil {
			panic(err)
		}
	}

	for {
		newContainer := false
		containers, err := cli.ContainerList(context.Background(), types.ContainerListOptions{
			All: true,
		})

		if err != nil {
			fmt.Println("Error listing containers: ", err)
			continue
		}

		for _, container := range containers {
			containerJson, err := cli.ContainerInspect(context.Background(), container.ID)
			if err != nil {
				fmt.Println("Error inspecting container: ", err)
				continue
			}

			// only store running containers
			if !containerJson.State.Running {
				continue
			}

			c := RunningContainer{
				Pid:       containerJson.State.Pid,
				Created:   containerJson.Created,
				StartedAt: containerJson.State.StartedAt,
				Image:     containerJson.Image,
				Info:      parseContainerLabels(containerJson.Config.Labels),
			}

			if !contains(runningContainers, c) {
				runningContainers = append(runningContainers, &c)
				newContainer = true
			}

			if newContainer {
				csvContents, err := gocsv.MarshalString(&runningContainers)

				if err != nil {
					panic(err)
				}
				// write csv to file
				file, err := os.OpenFile(resultFile, os.O_RDWR|os.O_CREATE, os.ModePerm)
				// file, err := os.OpenFile(resultFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModePerm)
				if err != nil {
					fmt.Println("Error opening file: ", err)
					continue
				}
				file.WriteString(csvContents)
				//file.WriteString(csvContent)
				fmt.Printf("New pod written: %s  Container: %s\n", c.Info.PodName, c.Info.ContainerName)
			}
			// wait 5 seconds
			time.Sleep(5 * time.Second)
		}
	}

}
