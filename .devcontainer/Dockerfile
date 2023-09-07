FROM mcr.microsoft.com/devcontainers/base:jammy

RUN apt-get -y update && apt-get -y upgrade

# Install librdkafka
RUN apt-get -y install librdkafka-dev libluajit-5.1-dev

# Install Tarantool development headers
RUN apt-get -y install gnupg2 curl apt-transport-https lsb-release gcc
RUN curl -L https://download.tarantool.org/tarantool/release/series-2/gpgkey | apt-key add -
RUN curl -L https://download.tarantool.org/tarantool/modules/gpgkey | apt-key add -
RUN rm -f /etc/apt/sources.list.d/*tarantool*.list
RUN echo "deb https://download.tarantool.org/tarantool/release/series-2/ubuntu/ $(lsb_release -c -s) main" | tee /etc/apt/sources.list.d/tarantool_2.list
RUN echo "deb-src https://download.tarantool.org/tarantool/release/series-2/ubuntu/ $(lsb_release -c -s) main" | tee -a /etc/apt/sources.list.d/tarantool_2.list
RUN echo "deb https://download.tarantool.org/tarantool/modules/ubuntu/ $(lsb_release -c -s) main" | tee -a /etc/apt/sources.list.d/tarantool_2.list
RUN echo "deb-src https://download.tarantool.org/tarantool/modules/ubuntu/ $(lsb_release -c -s) main" | tee -a /etc/apt/sources.list.d/tarantool_2.list
RUN apt-get -y update
RUN apt-get -y install tarantool-dev
