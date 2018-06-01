package net.corda.sandbox.rules.implementation

import net.corda.sandbox.code.Instruction
import net.corda.sandbox.code.instructions.MemberAccessInstruction
import net.corda.sandbox.rules.InstructionRule
import net.corda.sandbox.validation.RuleContext

/**
 * Rule that checks for illegal references to reflection API.
 */
@Suppress("unused")
class DisallowReflection : InstructionRule() {

    override fun validate(context: RuleContext, instruction: Instruction) = context.validate {
        fail("Disallowed reference to Class.newInstance()") given
                (instruction is MemberAccessInstruction && "java/lang/Class" in instruction.owner && instruction.memberName == "newInstance")
        fail("Disallowed invocation to reflection API") given
                (instruction is MemberAccessInstruction && "java/lang/reflect" in instruction.owner)

        // TODO Enable controlled use of reflection APIs
    }

}
