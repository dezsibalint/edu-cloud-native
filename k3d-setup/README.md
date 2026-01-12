# Local Kubernetes (k3s) environment for practices

## Summary

This folder contains scripts for creating an emulated local Kubernetes environment as a
backup for practices.

For the K3s environment, the scripts use the tool [k3d](https://github.com/k3d-io/k3d)
to create and configure a multi-node cluster over docker containers serving as K8s nodes 
on a single VM or laptop, which uses the docker-in-docker approach with some little magic
to initiate pods on them.

Currently, platforms based on **Ubuntu 24.04** are tested and used with these scripts. 

The scripts perform the following operations:
 - install requirements and system dependencies, including `docker`, `k3d`, and `kubectl`,
 - carry out minor validation checks of the deployment configuration,
 - creating a local Kubernetes cluster with names reflecting the given practice,
 - prepare the K8s environment for the practice by executing tailored setup steps,
e.g., custom image builds, etc.

## Scripts

- [setup_k3d_env.sh](setup_k3d_env.sh) - Install dependencies and perform validations.
- [practice_06_k8s_intro.sh](practice_06_k8s_intro.sh) - Set up an environment for Practice 06.

## Usage

### Preparation

To install and update the dependencies, execute the script
```bash
$ ./setup_k3d_env.sh -u
```

For other flags, try `setup_k3d_env.sh -h`.

This setup script needs to be executed only once in a VM.

### Practices

To create the environment of a given practice, execute the dedicated script 
with the name `practice_<num>_*.sh`.

For other features, e.g., tearing down the environment, check the help using `-h`.

For example,
```bash
# setup cluster
$ ./practice_06_k8s_intro.sh
# delete cluster
$ ./practice_06_k8s_intro.sh -d
# cleanup practice's intermediary data e.g., docker images  
$ ./practice_06_k8s_intro.sh -c
```

### Config

`k3d` automatically modifies the default cluster context, thus the `kubectl` command can be used
right after cluster creation without a fuzz.
This can be validated with the following command:
```bash
$ kubectl cluster-info
```

## Good to know

- At every cluster creation, the _current context_ is rewritten in the default kubeconfig file.
To check the available `k3d` cluster configs or select another context config, see `cat ~/.kube/config`
or consult with `kubectl config --help`.

- Custom-built docker images must be imported into the cluster using the `k3d image import` command.
 Usually, these steps are implicitly taken care by practice setup scritps!

- By default, `k3d` also initiates a load balancer as a separate container alongside the cluster
node containers for easier access to exposed services. Requests directed to the load balancer
are seamlessly proxied to the cluster nodes in the background.
For example, in case the web port is exposed on the load balancer by providing the `port` flag for
cluster creation with `-p 80:80@loadbalancer`), an "external service" can be accessed using
`http://localhost:80`.

- Configurations based on public domain names can be tested by using localhost domain names using
`*.localhost`. For example, to access to an ingress-exposed service via the load balancer, a URL
similar to `http://my-own-svc.my-own-subdomain.localhost:80` can be used.

- Ports exposed on nodes (e.g., `NodePort`) can be accessed via the load balancer or directly
on the "cluster node" containers provided it is set for `k3d` during cluster creation
(e.g.,`-p 30003:30003@loadbalancer` or `-p 30003:30003@server:0:direct`).
Check the related documentation [here](https://k3d.io/v5.8.3/usage/exposing_services/).

- Latest version _v5.8.3_ of `k3d` does not support _K8s Gateway API_ by default.
For an option to configure the _Gateway API_ with latest `traefik` version manually, check
[this description](https://doc.traefik.io/traefik/getting-started/kubernetes/).
Nevertheless, _Ingress_ routes work just fine.

## Differences in practices

### Practice 06 (K8s intro)

1. (Service) For accessing the created external service, you can use `127.0.0.1` or 
`localhost` as the "External IP".
2. (Ingress) For accessing the created ingress, configure and use an arbitrary `localhost`
domain, e.g., `my-service-http.practice06.localhost`.
