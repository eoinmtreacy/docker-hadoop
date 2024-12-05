FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

# MAINTAINER Ivan Ermilov <ivan.s.ermilov@gmail.com>

HEALTHCHECK CMD curl -f http://localhost:9870/ || exit 1

ENV HDFS_CONF_dfs_namenode_name_dir=file:///hadoop/dfs/name
RUN mkdir -p /hadoop/dfs/name
VOLUME /hadoop/dfs/name

RUN curl -O  https://dlcdn.apache.org/pig/pig-0.17.0/pig-0.17.0.tar.gz && \
    tar -xzvf pig-0.17.0.tar.gz  
    rm pig-0.17.0-src.tar.gz && \
    # ln -s /usr/local/pig-0.17.0 /usr/local/pig
    
ENV PATH=$PATH:/usr/local/pig/bin

ADD run.sh /run.sh
RUN chmod a+x /run.sh

EXPOSE 9870

CMD ["/run.sh"]
