#!/usr/bin/python
#Script para conectar como cliente MODBUS-TCP a inversor Huawei y publicar los valores en MQTT
#By JJOsuna. marzo 2020. Basado en script de Pedestre.
# String 2. enero 2021
#Angel ea4gkq julio 2021. Verifica si el socket esta abierto antes de lanzar la peticion modbus

from pymodbus.constants import Defaults
from pymodbus.client.sync import ModbusTcpClient as ModbusClient
import subprocess
import json
import time
import pdb
import os
import paho.mqtt.client as mqtt

fallos = 0
Defaults.Timeout = 1
def leerDetalles(ip_inverter):
    global fallos
    #intervalo de actualizacion en segundos
    intervalo=5
    lecturasMinuto=(60/intervalo)+1
    Defaults.Timeout = 1
    client = ModbusClient(ip_inverter, port=502, unit_id=0)
    client.connect()
    if client.connect():
        time.sleep(1)
        cont=1
        while cont<lecturasMinuto:
            potenciaPico=-1
            while potenciaPico<0:
                try:
                   if(client.is_socket_open()):
                     energiaExportada,activePowerInv,energiaDiaria,potenciaPico,String_1_Voltaje,String_1_Corriente,String_2_Voltaje,String_2_Corriente,activePowerPaneles,TempInversor,VoltajeINV=leerDelInversor(client)
                   else:
                     #print("socket cerrado.")
                     client.close()
                     client = ModbusClient(ip_inverter, port=502, unit_id=0)
                     client.connect()
                     time.sleep(1)
                except:
                    client = ModbusClient(ip_inverter, port=502, unit_id=0)
                    client.connect()
                    time.sleep(1)
                    energiaExportada,activePowerInv,energiaDiaria,potenciaPico,String_1_Voltaje,String_1_Corriente,String_2_Voltaje,String_2_Corriente,activePowerPaneles,TempInversor,VoltajeINV=leerDelInversor(client)
                    fallos = fallos + 1
                    #print("ha fallado")
            try:
                #print("Eneriga: "+str(energiaExportada)+"w Fallos ModBus: "+str(fallos))
                clientMQTT.publish(topic="emon/NodeHuawei/Meter", payload= str(energiaExportada), qos=1, retain=False)
                clientMQTT.publish(topic="emon/NodeHuawei/EnergiaDia", payload=str(energiaDiaria/100.0), qos=1, retain=False)
                clientMQTT.publish(topic="emon/NodeHuawei/Power_inversor", payload=str(activePowerInv), qos=1, retain=False)
                clientMQTT.publish(topic="emon/NodeHuawei/Power_pico", payload=str(potenciaPico), qos=1, retain=False)
                clientMQTT.publish(topic="emon/NodeHuawei/String_1_Voltaje", payload=str(String_1_Voltaje/10.0), qos=1, retain=False)
                clientMQTT.publish(topic="emon/NodeHuawei/String_1_Corriente", payload=str(String_1_Corriente/100.0), qos=1, retain=False)
                clientMQTT.publish(topic="emon/NodeHuawei/String_2_Voltaje", payload=str(String_2_Voltaje/10.0), qos=1, retain=False)
                clientMQTT.publish(topic="emon/NodeHuawei/String_2_Corriente", payload=str (String_2_Corriente/100.0), qos=1, retain=False)
                clientMQTT.publish(topic="emon/NodeHuawei/Power_paneles", payload=str(activePowerPaneles), qos=1, retain=False)
                clientMQTT.publish(topic="emon/NodeHuawei/Temp_Inversor", payload=str(TempInversor/10.0), qos=1, retain=False)
                clientMQTT.publish(topic="emon/NodeHuawei/VoltajeINV", payload=str(VoltajeINV/10.0), qos=1, retain=False)
                #clientMQTT.publish(topic="emon/NodeHuawei/String_2_Voltaje", payload=str(String_2_Voltaje/10.0), qos=1, retain=False)
                #clientMQTT.publish(topic="emon/NodeHuawei/String_2_Corriente", payload=str(String_2_Corriente/100.0), qos=1, retain=False)
                #print("Lectura hecha")
            except (IOError, OSError):                
                fallos = fallos + 1
                #print("ha fallado")
                pass                

            time.sleep(intervalo)
            cont += 1
            if cont==lecturasMinuto:
                cont=1
            if fallos > 5:
              try:
               sys.exit(0)
              except SystemExit:
               os._exit(0)
def leerDelInversor(client):
    global fallos
    try:
        #Leer la potencia solar actual
        rr = client.read_holding_registers(0x7D50, 0x02) #32080 POTENCIA FV
        activePowerInv=rr.registers[1]

        #Leer la lectura del meter (exported2 - exported1) positivo exportando, negativo consumiendo
        rr1 = client.read_holding_registers(0x90f9, 0x02) #37113 CONSUMO TOTAL
        #rr1 = client.read_holding_registers(0x9cb8, 0x01) #40120
        exported1=rr1.registers[0]
        exported2=rr1.registers[1]
        #pdb.set_trace()
        energiaExportada=exported2-exported1

        #Leer la produccion del dia
        rr = client.read_holding_registers(0x7d72, 0x02) #32114
        energiaDiaria=rr.registers[1]

        #Leer Pico de produccion del dia
        rr = client.read_holding_registers(0x7d4e, 0x02) #32078
        potenciaPico=rr.registers[1]

        #Leer Voltaje String 1
        rr = client.read_holding_registers(0x7D10, 0x01) #32016
        String_1_Voltaje=rr.registers[0]

        #Leer Corriente String 1
        rr = client.read_holding_registers(0x7D11, 0x01) #32017
        String_1_Corriente=rr.registers[0]

        #Leer Voltaje String 2
        rr = client.read_holding_registers(0x7D12, 0x01) #32018
        String_2_Voltaje=rr.registers[0]

        #Leer Corriente String 2
        rr = client.read_holding_registers(0x7D13, 0x01) #32019
        String_2_Corriente=rr.registers[0]

        #Leer la potencia paneles solares que entra en inversor
        rr = client.read_holding_registers(0x7D40, 0x02) #32064
        activePowerPaneles=rr.registers[1]

       #Leer temperatura interna inversor
        rr = client.read_holding_registers(0x7D57, 0x01) #32087
        TempInversor=rr.registers[0]

       #Leer Voltaje de salida del inversor
        rr = client.read_holding_registers(0x7D42, 0x01) #32066
        VoltajeINV=rr.registers[0]
        fallos = 0

        return energiaExportada,activePowerInv,energiaDiaria,potenciaPico,String_1_Voltaje,String_1_Corriente,String_2_Voltaje,String_2_Corriente,activePowerPaneles,TempInversor,VoltajeINV
    except:
        fallos = fallos + 1    
        return 0,0,0,-1,0,0,0,0,0,0,0

def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Unexpected MQTT disconnection. Will auto-reconnect")
        
def on_connect(client, userdata, flags, rc):
    if rc==0: 
        client.connected_flag=True #set flag print("conectado OK") else: print("No conectado, codigo retornado=",rc)
        print("MQTT connected")
mqtt.Client.connected_flag=False#create flag in class
#poner a continuacion la IP de la raspberry
broker_url = "192.168.1.20"
broker_port = 1883

clientMQTT = mqtt.Client()
clientMQTT.on_connect=on_connect #bind call back function
clientMQTT.on_disconnect = on_disconnect
clientMQTT.loop_start()
#Some Executable Code Here
print("Connecting to broker ",broker_url)
clientMQTT.username_pw_set(username="user",password="password")
clientMQTT.connect(broker_url, broker_port) #connect to broker
while not clientMQTT.connected_flag: #wait in loop
    print("In wait loop")
    time.sleep(1)
print("in Main Loop")

#poner la IP del inversor
leerDetalles("192.168.1.58")

clientMQTT.loop_stop()
