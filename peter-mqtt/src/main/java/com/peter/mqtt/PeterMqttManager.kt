package com.peter.mqtt

import android.content.Context
import android.util.Log
import com.peter.mqtt.bean.ConnectState
import com.peter.mqtt.bean.Event
import com.peter.mqtt.bean.TopicMessage
import info.mqtt.android.service.MqttAndroidClient
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.launch
import org.eclipse.paho.client.mqttv3.DisconnectedBufferOptions
import org.eclipse.paho.client.mqttv3.IMqttActionListener
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken
import org.eclipse.paho.client.mqttv3.IMqttToken
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended
import org.eclipse.paho.client.mqttv3.MqttConnectOptions
import org.eclipse.paho.client.mqttv3.MqttMessage


class PeterMqttManager private constructor() {

    private lateinit var mqttAndroidClient: MqttAndroidClient
    private var isDebug: Boolean = false
    private val mTopics = mutableMapOf<String, Int>()

    companion object {

        private val job by lazy { Job() }
        private val scope by lazy { CoroutineScope(job) }

        @Volatile
        private var instance: PeterMqttManager? = null
        fun getInstance(): PeterMqttManager {
            return instance ?: synchronized(this) {
                instance ?: PeterMqttManager().also { instance = it }
            }
        }

        private val _sharedRawDataFlow =
            MutableSharedFlow<TopicMessage>(replay = 1, extraBufferCapacity = 1)
        val rawDataFlow: SharedFlow<TopicMessage> get() = _sharedRawDataFlow

        private val _sharedConnectStateFlow =
            MutableSharedFlow<ConnectState>(replay = 1, extraBufferCapacity = 1)
        val connectStateFlow: SharedFlow<ConnectState> get() = _sharedConnectStateFlow

        private fun sendEvent(event: Event) {
            if (event is TopicMessage) {
                scope.launch {
                    _sharedRawDataFlow.emit(event)
                }
            }
        }

        private fun updateConnectState(state: ConnectState) {
            scope.launch {
                _sharedConnectStateFlow.emit(state)
            }
        }

        fun topics(vararg topic: String): Flow<TopicMessage> {
            return rawDataFlow.filter { tc ->
                tc.topic in topic
            }
        }

        fun listen(vararg topic: String, call: (TopicMessage) -> Unit) {
            scope.launch {
                rawDataFlow.filter { tc ->
                    if (topic.isEmpty()) true else tc.topic in topic
                }.filter { tc ->
                    call.invoke(tc)
                    true
                }.collect()
            }
        }

        fun listenConnectState(call: (ConnectState) -> Unit) {
            scope.launch {
                connectStateFlow.filter { tc ->
                    call.invoke(tc)
                    true
                }.collect()
            }
        }

    }

    fun init(
        context: Context,
        serviceUrl: String,
        clientId: String? = null,
        userName: String? = null,
        passwd: String? = null,
        isDebug: Boolean = false
    ) {
        this.isDebug = isDebug
        mqttAndroidClient = MqttAndroidClient(
            context,
            serviceUrl,
            clientId ?: "PeterMQ_${System.currentTimeMillis()}"
        )
        mqttAndroidClient.setCallback(object : MqttCallbackExtended {
            override fun connectComplete(reconnect: Boolean, serverURI: String) {
                if (reconnect) {
                    // Because Clean Session is true, we need to re-subscribe
                    subscribeToTopic(false)
                    updateConnectState(ConnectState.CONNECTED)
                    log("Reconnected: $serverURI")
                } else {
                    log("Connected: $serverURI")
                    updateConnectState(ConnectState.CONNECTED)
                }
            }

            override fun connectionLost(cause: Throwable?) {
                log("The Connection was lost.")
                updateConnectState(ConnectState.DISCONNECT)
            }

            override fun messageArrived(topic: String, message: MqttMessage) {
                log("Incoming message: " + String(message.payload))
            }

            override fun deliveryComplete(token: IMqttDeliveryToken) = Unit
        })
        val mqttConnectOptions = MqttConnectOptions()
        mqttConnectOptions.isAutomaticReconnect = true
        mqttConnectOptions.isCleanSession = false
        mqttConnectOptions.userName = userName
        mqttConnectOptions.password = passwd?.toCharArray()
        log("Connecting: $serviceUrl")
        updateConnectState(ConnectState.CONNECTING)
        mqttAndroidClient.connect(mqttConnectOptions, null, object : IMqttActionListener {
            override fun onSuccess(asyncActionToken: IMqttToken) {
                val disconnectedBufferOptions = DisconnectedBufferOptions().apply {
                    isBufferEnabled = true
                    bufferSize = 100
                    isPersistBuffer = false
                    isDeleteOldestMessages = false
                }
                mqttAndroidClient.setBufferOpts(disconnectedBufferOptions)
                subscribeToTopic(false)
            }

            override fun onFailure(asyncActionToken: IMqttToken?, exception: Throwable?) {
                log("Failed to connect: $serviceUrl")
                updateConnectState(ConnectState.DISCONNECT)
            }
        })
    }

    fun subscribeToTopic(removeHistory: Boolean, vararg topics: Pair<String, Int>) {

        if (removeHistory) {
            mqttAndroidClient.unsubscribe(mTopics.map { it.key }.toTypedArray())
            mTopics.clear()
        }

        if (mTopics.isEmpty()) {
            log("Not hav topic!")
            return
        }

        mqttAndroidClient.subscribe(
            mTopics.map { it.key }.toTypedArray(),
            mTopics.map { it.value }.toIntArray(),
            null,
            object : IMqttActionListener {
                override fun onSuccess(asyncActionToken: IMqttToken?) {
                    log("Subscribed! ${asyncActionToken?.topics}")
                }

                override fun onFailure(asyncActionToken: IMqttToken?, exception: Throwable?) {
                    log("Failed to subscribe $exception")
                }

            })
    }

    private fun log(string: String) {
        if (isDebug) {
            Log.d("PeterMqtt", string)
        }
    }

}