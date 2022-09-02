FROM hseeberger/scala-sbt:graalvm-ce-21.3.0-java11_1.6.2_3.1.1

WORKDIR /home/app
ADD project /home/app
ADD build.sbt /home/app

RUN sbt update

ADD . /home/app

RUN sbt compile

ENV DATA_PATH="/data"
ENV BOOTSTRAP_SERVERS="kafka1:9092"
ENV TIME_FACTOR="0.001"

CMD sbt run
