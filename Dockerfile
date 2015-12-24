FROM beget/sprutio-python
MAINTAINER "Maksim Losev <mlosev@beget.ru>"

RUN apt-get install --no-install-recommends -qq -y \
    unzip zip

COPY requirements.txt /
RUN pip install -r /requirements.txt

COPY run-rpc.sh /etc/services.d/rpc/run
COPY run-sendfile.sh /etc/services.d/sendfile/run

COPY init-db.sh /etc/cont-init.d/10-init-db.sh
COPY init-tmp.sh /etc/cont-init.d/20-init-tmp.sh
COPY init-passwd.sh /etc/cont-init.d/30-init-passwd.sh

COPY ./ /rpc/

WORKDIR /rpc/
