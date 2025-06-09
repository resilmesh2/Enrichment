FROM python:3.9 AS enrichment
LABEL authors="jorgeley@silentpush.com"

#RUN echo 'https://dl-3.alpinelinux.org/alpine/v3.4/main' > /etc/apk/repositories  && \
#    echo '@testing https://dl-3.alpinelinux.org/alpine/edge/testing' >> /etc/apk/repositories && \
#    echo '@community https://dl-3.alpinelinux.org/alpine/v3.4/community'
RUN apt -y update
#RUN apt -y install procps iputils* net-tools
RUN adduser app --system --home /home/app
COPY --chmod=755 ./sp-enrichment/ /home/app/sp-enrichment
COPY --chmod=755 entrypoint.sh /home/app/
RUN chown -R app:nogroup /home/app
RUN chmod 755 -R /home/app

USER app
WORKDIR /home/app
RUN pip install --upgrade pip
RUN pip3 install -r sp-enrichment/requirements.txt
RUN mkdir -p \
    events/inbound/ipv4s \
    events/inbound/ipv6s \
    events/inbound/domains \
    events/inbound/unknown
RUN mkdir -p \
    events/outbound/ipv4s \
    events/outbound/ipv6s \
    events/outbound/domains \
    events/outbound/unknown
RUN touch publisher.log
RUN chmod 755 entrypoint.sh
# RUN pwd ; ls -la . ; ls -laR sp-enrichment
ENTRYPOINT /home/app/entrypoint.sh