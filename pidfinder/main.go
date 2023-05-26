package main

import (
	"context"
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
	Pid     int
	Created string
	Info    PodLabelsInfo
}

var RESULT_PATH = ""

type RunningContainers []RunningContainer

func parseContainerLabels(labels map[string]string) PodLabelsInfo {
	var podLabels PodLabelsInfo
	podLabels.PodName = labels["io.kubernetes.pod.name"]
	podLabels.ContainerName = labels["io.kubernetes.container.name"]
	podLabels.PodNamespace = labels["io.kubernetes.pod.namespace"]
	podLabels.App = labels["app"]
	podLabels.NextflowRunName = labels["runName"]
	podLabels.NextflowProcessName = labels["processName"]
	podLabels.NextflowSessionID = labels["sessionId"]
	podLabels.NextflowTaskName = labels["taskName"]
	return podLabels
}

func contains(l []*RunningContainer, c RunningContainer) bool {
	for _, con := range l {
		if con.Pid == c.Pid {
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

	for {
		newContainer := false
		containers, err := cli.ContainerList(context.Background(), types.ContainerListOptions{})
		if err != nil {
			panic(err)
		}

		runningContainers := []*RunningContainer{}
		for _, container := range containers {
			containerJson, err := cli.ContainerInspect(context.Background(), container.ID)
			if err != nil {
				panic(err)
			}

			c := RunningContainer{
				Pid:     containerJson.State.Pid,
				Created: containerJson.Created,
				Info:    parseContainerLabels(containerJson.Config.Labels),
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
				if err != nil {
					panic(err)
				}
				file.WriteString(csvContents)
			}
			// wait 5 seconds
			time.Sleep(5 * time.Second)
		}
	}

}
