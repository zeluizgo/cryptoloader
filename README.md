# cryptoloader
ETL de carga de dados do mercado crypto em pasta local.

Esta pasta local será na realidade uma pasta distribuida (através de glusterfs) e integrada a um sistema de arquivos hadoop permitindo assim seu uso em processamento paralelo via Spark, agilizando o cálculo de indicadores, análises e integração com IA.

Pré-requisitos: 

# 1. Instalação do glusterfs em cada nó do cluster de servidores:
A instalação do glusterfs é simples basta seguir o guia de instalação na documentação do gluster: 
https://docs.gluster.org/en/main/Install-Guide/Install/#installing-gluster

No entanto, se sua distribuição do ubuntu for mais recente vale buscar instruções espec[ificas de instalação (no meu caso foi o ubuntu 24.04).

Ao final vale também liberar a comunicação entre os servidores especificando seus IPs:
```bash
iptables -I INPUT -p all -s `<ip-address>` -j ACCEPT
```

# 2. Instalação do plugin glusterfs-volume-plugin do mochoa 

É necessário garantir que em cada nó do cluster o plugin do docker que acessa o volume gluster esteja instalado, para isso utilizaremos o ***glusterfs-volume-plugin*** , explore em:
https://hub.docker.com/r/mochoa/glusterfs-volume-plugin

```bash
# On each Swarm node:
docker plugin install --alias glustermochhoa mochoa/glusterfs-volume-plugin-armv7l --grant-all-permissions --disable

docker plugin set glustermochhoa SERVERS=ubuntu-pi-01,ubuntu-pi-02,ubuntu-pi-03

docker plugin enable glustermochhoa
```
 




