package integration

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/docker/distribution/reference"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

var (
	defaultTimeout = 10 * time.Second
)

// NewDockerClient returns a docker client
func NewDockerClient() (*dockerClient, error) {
	cli, err := client.NewEnvClient()
	if err != nil {
		return nil, fmt.Errorf("unable to create docker client: %w", err)
	}
	return &dockerClient{*cli}, nil
}

type dockerClient struct {
	client.Client
}

type ContainerConfig struct {
	image string
	ports []*PortMapping
	env   []string
	cmd   []string
}

type PortMapping struct {
	HostPort      string
	ContainerPort string
	Proto         string
}

func (d dockerClient) runContainer(ctx context.Context, config *ContainerConfig) (*container.ContainerCreateCreatedBody, error) {
	imageName, err := reference.ParseNormalizedNamed(config.image)
	if err != nil {
		return nil, fmt.Errorf("unable to normalize image name: %w", err)
	}
	fullName := imageName.String()

	out, err := d.ImagePull(ctx, fullName, types.ImagePullOptions{})
	if err != nil {
		return nil, fmt.Errorf("unable to pull image: %w", err)
	}
	defer out.Close()

	io.Copy(os.Stdout, out)

	container, err := d.createNewContainer(ctx, fullName, config.ports, config.env, config.cmd)
	if err != nil {
		return nil, fmt.Errorf("unable create container: %w", err)
	}
	err = d.ContainerStart(ctx, container.ID, types.ContainerStartOptions{})
	if err != nil {
		return nil, fmt.Errorf("unable to start the container: %w", err)
	}
	fmt.Printf("container %s is started\n", container.ID)
	return container, nil
}

func (d dockerClient) createNewContainer(ctx context.Context, image string, ports []*PortMapping, env []string, cmd []string) (*container.ContainerCreateCreatedBody, error) {
	portBinding := nat.PortMap{}
	for _, portmap := range ports {
		hostBinding := nat.PortBinding{
			//TODO: Allow for host ips to be specified
			HostIP:   "0.0.0.0",
			HostPort: portmap.HostPort,
		}
		containerPort, err := nat.NewPort("tcp", portmap.ContainerPort)
		if err != nil {
			return nil, fmt.Errorf("unable to get the port: %w", err)
		}
		portBinding[containerPort] = []nat.PortBinding{hostBinding}
	}

	containerConfig := &container.Config{
		Image: image,
		Env:   env,
	}
	if len(cmd) > 0 {
		containerConfig.Cmd = cmd
	}

	hostConfig := &container.HostConfig{
		PortBindings: portBinding,
	}
	networkingConfig := &network.NetworkingConfig{}
	cont, err := d.ContainerCreate(ctx, containerConfig, hostConfig, networkingConfig, nil, "")

	if err != nil {
		return nil, fmt.Errorf("could not create container: %w", err)
	}
	return &cont, nil
}

func (d dockerClient) removeContainer(ctx context.Context, id string) error {
	fmt.Printf("container %s is stopping\n", id)
	err := d.ContainerStop(ctx, id, &defaultTimeout)
	if err != nil {
		return fmt.Errorf("failed stopping container: %w", err)
	}
	fmt.Printf("container %s is stopped\n", id)

	err = d.Client.ContainerRemove(ctx, id, types.ContainerRemoveOptions{
		RemoveVolumes: true,
		// RemoveLinks=true causes "Error response from daemon: Conflict, cannot
		// remove the default name of the container"
		RemoveLinks: false,
		Force:       false,
	})
	if err != nil {
		return fmt.Errorf("failed removing container: %w", err)
	}
	fmt.Printf("container %s is removed\n", id)
	return nil
}

func (d dockerClient) printLogs(ctx context.Context, id string) error {
	out, err := d.ContainerLogs(ctx, id, types.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
		Timestamps: true,
	})
	if err != nil {
		return err
	}
	io.Copy(os.Stdout, out)
	defer out.Close()
	return nil
}
