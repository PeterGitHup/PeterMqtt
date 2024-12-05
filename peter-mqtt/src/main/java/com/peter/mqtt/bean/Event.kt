package com.peter.mqtt.bean

import org.eclipse.paho.client.mqttv3.MqttMessage

interface Event

data class TopicMessage(
    val topic: String,
    val message: MqttMessage? = null,
    val time: Long = 0
) : Event

sealed interface ConnectState {
    data object CONNECTED : ConnectState
    data object CONNECTING : ConnectState
    data object DISCONNECT : ConnectState
}