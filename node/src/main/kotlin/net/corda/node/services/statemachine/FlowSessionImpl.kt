package net.corda.node.services.statemachine

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.flows.*
import net.corda.core.identity.Party
import net.corda.core.internal.FlowStateMachine
import net.corda.core.utilities.UntrustworthyData
import net.corda.core.utilities.unwrap
import net.corda.node.services.statemachine.metered.MessageAndMetrics
import net.corda.node.services.statemachine.metered.Metered
import org.slf4j.LoggerFactory
import java.io.IOException

class FlowSessionImpl(override val counterparty: Party) : FlowSession() {
    internal lateinit var stateMachine: FlowStateMachine<*>
    internal lateinit var sessionFlow: FlowLogic<*>

    var otherPartySpentOnCheckpointing: Long = 0
    @Transient val log = LoggerFactory.getLogger(FlowSessionImpl::class.java)

    @Suspendable
    override fun getCounterpartyFlowInfo(maySkipCheckpoint: Boolean): FlowInfo {
        return stateMachine.getFlowInfo(counterparty, sessionFlow, maySkipCheckpoint)
    }

    @Suspendable
    override fun getCounterpartyFlowInfo() = getCounterpartyFlowInfo(maySkipCheckpoint = false)

    @Suspendable
    override fun <R : Any> sendAndReceive(
            receiveType: Class<R>,
            payload: Any,
            maySkipCheckpoint: Boolean
    ): UntrustworthyData<R> {
        // TODO - make metered implementation for this, for POC skip optimization
//        return stateMachine.sendAndReceive(
//                receiveType,
//                counterparty,
//                payload,
//                sessionFlow,
//                retrySend = false,
//                maySkipCheckpoint = maySkipCheckpoint
//        )
        send(payload, maySkipCheckpoint)
        return receive(receiveType, maySkipCheckpoint)
    }

    @Suspendable
    override fun <R : Any> sendAndReceive(receiveType: Class<R>, payload: Any) = sendAndReceive(receiveType, payload, maySkipCheckpoint = false)

    @Suspendable
    override fun <R : Any> receive(receiveType: Class<R>, maySkipCheckpoint: Boolean): UntrustworthyData<R> {
        logThisFlowName()
        // Kryo is unable to deserialize stuff from Kotlin reflect -> ::class.findAnnotation<Metered>() doesn't work
        if (!isMetered(sessionFlow.javaClass)
                || !sessionFlow.javaClass.isAnnotationPresent(InitiatingFlow::class.java)) {
            log.info("RECEIVE NORMAL")
            return stateMachine.receive(receiveType, counterparty, sessionFlow, maySkipCheckpoint)
        }
        else {
            // TODO - receiving message must be decoupled from deserialization to make this code great again
            val message = (stateMachine as FlowStateMachineImpl).receiveRaw(counterparty, sessionFlow)
            return try {
                val messageAndMetrics = message.checkPayloadIs(MessageAndMetrics::class.java)
                val unwrappedMessage = receiveType.cast(messageAndMetrics.unwrap { it.message })
                otherPartySpentOnCheckpointing += messageAndMetrics.unwrap { it.metric }
                log.info("RECEIVE WRAPPED")
                UntrustworthyData(unwrappedMessage)
            }
            catch (e: IOException) {
                processNonMetered(message, receiveType)
            }
            catch (e: UnexpectedFlowEndException) {
                processNonMetered(message, receiveType)
            }
        }
    }

    private fun thisIsMetered(): Boolean {
        return isMetered(sessionFlow.javaClass)
    }

    private fun isMetered(flowClass: Class<FlowLogic<*>>): Boolean {
        return flowClass.isAnnotationPresent(Metered::class.java)
                || flowClass.`package`.name.startsWith("net.corda")
    }

    private fun logThisFlowName() {
        log.info(sessionFlow.javaClass.name + " :: " + thisIsMetered())
    }

    private fun <R : Any> processNonMetered(message: DataSessionMessage, receiveType: Class<R>): UntrustworthyData<R> {
        log.info("RECEIVE NORMAL")
        return message.checkPayloadIs(receiveType)
    }

    @Suspendable
    override fun <R : Any> receive(receiveType: Class<R>) = receive(receiveType, maySkipCheckpoint = false)

    @Suspendable
    override fun send(payload: Any, maySkipCheckpoint: Boolean): Unit {
        logThisFlowName()
        // Kryo is unable to deserialize stuff from Kotlin reflect -> ::class.findAnnotation<Metered>() doesn't work
        if (!isMetered(sessionFlow.javaClass)
                || !sessionFlow.javaClass.isAnnotationPresent(InitiatedBy::class.java)) {
            // TODO - this requires more logic based on counterParty as some flows can be both Initiated and Initiating
            // TODO - also flow must not wrap metrics is flow was initiated by legacy node without metric support
            log.info("SEND NORMAL")
            stateMachine.send(counterparty, payload, sessionFlow, maySkipCheckpoint)
        }
        else {
            log.info("SEND WRAPPED")
            val timestamps = (stateMachine as FlowStateMachineImpl).checkpointTimestamps

            fun Int.isEven(): Boolean {
                return (this % 2) == 0
            }

            var checkpointDurationMs = 0L
            for (i in 0..(timestamps.size - 1)) {
                val epochMillis = timestamps[i].toEpochMilli()
                if (i.isEven()) {
                    checkpointDurationMs -= epochMillis
                }
                else {
                    checkpointDurationMs += epochMillis
                }
            }

            val log = LoggerFactory.getLogger(javaClass)
            log.info("Collected checkpoint timestamps: {}", timestamps)
            log.info("Collected checkpoint total duration: {}", checkpointDurationMs)

            stateMachine.send(
                    counterparty,
                    MessageAndMetrics(payload, checkpointDurationMs),
                    sessionFlow,
                    maySkipCheckpoint)
        }
    }

    @Suspendable
    override fun send(payload: Any) = send(payload, maySkipCheckpoint = false)

    override fun toString() = "Flow session with $counterparty"
}

