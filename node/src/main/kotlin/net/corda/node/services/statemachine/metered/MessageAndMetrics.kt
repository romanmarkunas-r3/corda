package net.corda.node.services.statemachine.metered

import net.corda.core.serialization.CordaSerializable

@CordaSerializable
class MessageAndMetrics(
        val message: Any,
        val metric: Long
)
