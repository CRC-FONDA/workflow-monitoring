# Low-level I/O Monitoring for Scientific Workflows

The repository has the following structure:

- ./bpf: contains files for the eBPF monitoring and for logging cgroups and parent pids of spawned processes
- ./cluster: contains nextflow configuration and kubernetes definitions used for testing the eBPF based monitoring in our cluster
- ./data: contains scripts to import the raw monitoring data (.csv) into a sqlite database or prometheus; also contains a script using that data in a sqlite database for evaluation and creating the figures as seen in the paper
- ./fuse: contains the implementation of the overlay monitoring filesystem
- ./pidfinder: contains a solution written in go for connecting pods of nextflow tasks to pids
