#FROM debian
FROM ubuntu:22.04
WORKDIR /
RUN apt update

# to get the bcc code
RUN apt install -y git
# bcc dependencies
RUN apt install -y \
    arping \
    bison \
    clang-format\ 
    cmake\ 
    dh-python\ 
    dpkg-dev\ 
    pkg-kde-tools\ 
    ethtool\ 
    flex\ 
    inetutils-ping\ 
    iperf\ 
    libbpf-dev\ 
    libclang-dev\ 
    libclang-cpp-dev\ 
    libedit-dev\ 
    libelf-dev\ 
    libfl-dev\ 
    libzip-dev\ 
    linux-libc-dev\ 
    llvm-dev\ 
    libluajit-5.1-dev\ 
    luajit\ 
    python3-netaddr\ 
    python3-pyroute2\ 
    python3-distutils\ 
    python3 \
    zip

RUN apt-get install -y linux-headers-$(uname -r)

RUN git clone -b master --single-branch --depth 1 https://github.com/iovisor/bcc.git && \
    cd bcc && \
    mkdir build && \
    cd build && \
    cmake .. && \
    make -j && \
    make install && \
    rm -rf /bcc

WORKDIR /bpf

COPY fileaccess.py .
COPY testdir ./testdir

COPY entrypoint.sh /
ENTRYPOINT [ "/entrypoint.sh" ]
