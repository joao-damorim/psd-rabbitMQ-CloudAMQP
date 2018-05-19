#!/usr/bin/env python
import pika
import sys
import time

if len(sys.argv) != 4 :
    print >> sys.stderr, "Usage: %s [CloudAMQP Host] [CloudAMQP: User & Vhost] [CloudAMQP Password]..." % (sys.argv[0],)
    sys.exit(1)

host = sys.argv[1]
user_vhost = sys.argv[2]
passwd = sys.argv[3]

credentials = pika.PlainCredentials(user_vhost, passwd)
connection = pika.BlockingConnection(pika.ConnectionParameters(host, 5672, user_vhost, credentials))
channel = connection.channel()

channel.exchange_declare(exchange='topic_logs',
exchange_type='topic')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='topic_logs',
                        queue=queue_name,
                        routing_key='*.*')

print ' [*] Waiting for messages. To exit press CTRL+C'

def alertar (alerta, mensagem):
    print("alertou")
    channel.basic_publish(exchange = 'topic_logs',
                            routing_key = alerta,
                            body = mensagem)

def callback(ch, method, properties, body):
    print " [x] Received %r %r" % (method.routing_key, body)
    cabecalho = method.routing_key
    lista_cabecalho = cabecalho.split(".")
    cidade = lista_cabecalho[0]
    tipo_do_sensor = lista_cabecalho[1]
    corpo = body
    lista_corpo = body.split(",")
    timestamp = lista_corpo[0]
    valor_do_sensor = lista_corpo[1]
    valor_do_sensor_int = int(lista_corpo[1])
    if (tipo_do_sensor == "precipitacao" and valor_do_sensor_int > 100):
        tipo_do_alerta = "inundacao"
        alerta = str(cidade+"."+"alerta"+"."+tipo_do_alerta)
        print (alerta)
        alertar(alerta, valor_do_sensor)

    if (tipo_do_sensor == "velocidade-vento" and valor_do_sensor_int > 100):
        tipo_do_alerta = "ventania"
        alerta = str(cidade+"."+"alerta"+"."+tipo_do_alerta)
        print (alerta)
        alertar(alerta, valor_do_sensor)

    if (tipo_do_sensor == "radiacao-uv" and valor_do_sensor_int > 5):
        tipo_do_alerta = "insolacao"
        alerta = str(cidade+"."+"alerta"+"."+tipo_do_alerta)
        print (alerta)
        alertar(alerta, valor_do_sensor)

channel.basic_consume(callback,
                      queue=queue_name,
                      no_ack=True)

channel.start_consuming()
