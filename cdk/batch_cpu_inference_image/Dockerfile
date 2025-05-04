FROM 763104351884.dkr.ecr.us-west-2.amazonaws.com/pytorch-inference:2.6.0-cpu-py312-ubuntu22.04-ec2

RUN apt update
RUN apt -y install unzip 
RUN apt -y install curl 

RUN pip3 install boto3
RUN pip3 install dataset
RUN pip3 install transformers
RUN pip3 install sentence-transformers
RUN pip3 install scikit-learn
RUN pip3 install numpy
RUN pip3 install networkx

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install

COPY python-from-s3.sh /
ENTRYPOINT [ "/python-from-s3.sh" ]
    