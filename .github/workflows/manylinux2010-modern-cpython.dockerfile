# syntax=docker/dockerfile:1
FROM quay.io/pypa/manylinux2010_x86_64:latest
RUN curl https://pyenv.run | bash && \
 export PYENV_ROOT="$HOME/.pyenv" && \
 export PATH="$PYENV_ROOT/bin:$PATH" && \
 eval "$(pyenv init -)" && \
 yum install -y make patch zlib-devel bzip2 bzip2-devel readline-devel sqlite sqlite-devel openssl-devel tk-devel libffi-devel xz-devel perl-IPC-Cmd && \
 curl -L https://www.openssl.org/source/openssl-3.0.12.tar.gz >openssl-3.0.12.tar.gz && \
 tar xzf openssl-3.0.12.tar.gz && \
 (cd openssl-3.0.12 && ./config no-shared --prefix=/usr/local/ssl --openssldir=/usr/local/ssl --libdir=lib && make && make install_sw) && \
 rm -rf openssl-3.0.12.tar.gz && \
 rm -rf openssl-3.0.12 && \
 PYTHON_CONFIGURE_OPTS=--with-openssl=/usr/local/ssl pyenv install 3.11 && \
 ln -s /root/.pyenv/versions/3.11* /opt/python/cp311-cp311 && \
 PYTHON_CONFIGURE_OPTS=--with-openssl=/usr/local/ssl pyenv install 3.12 && \
 ln -s /root/.pyenv/versions/3.12* /opt/python/cp312-cp312 && \
 true
