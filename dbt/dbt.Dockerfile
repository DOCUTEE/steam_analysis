FROM python:3.11-slim

RUN apt-get update && apt-get install -y git  # <--- Cần git
RUN pip install dbt-core dbt-spark dbt-spark[PyHive] dbt-spark[session]

RUN apt-get update && apt-get install -y openssh-server

RUN mkdir -p /var/run/sshd

# Set root password
RUN echo "root:root" | chpasswd

RUN sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

CMD ["/usr/sbin/sshd", "-D"]

EXPOSE 22

RUN mkdir -p /dbt
WORKDIR /dbt
