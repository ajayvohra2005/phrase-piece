FROM ubuntu:jammy

RUN apt update
RUN apt -y install build-essential
RUN apt -y install python3-minimal
RUN apt -y install python3-pip
RUN apt -y install unzip 
RUN apt -y install curl 

RUN pip3 install boto3

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install

COPY python-from-s3.sh /
ENTRYPOINT [ "/python-from-s3.sh" ]
    