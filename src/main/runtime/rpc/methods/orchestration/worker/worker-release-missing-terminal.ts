import type { OrchestrationDb } from '../../../../orchestration/db'
import {
  decideWorkerTerminalRelease,
  type WorkerTerminalResourceRow
} from '../../../../orchestration/worker-terminal-ownership'
import type { OrcaRuntimeService } from '../../../../orca-runtime'
import type { WorkerReleaseReceipt } from './worker-release-completion'
import { inspectWorkerTerminal } from './worker-observation'
import { archiveSummary } from './worker-terminal-resource-presentation'

export async function releaseProvenDeadMissingWorkerTerminal(args: {
  runtime: OrcaRuntimeService
  db: OrchestrationDb
  dispatchId: string
  resource: WorkerTerminalResourceRow
}): Promise<WorkerReleaseReceipt | null> {
  const { runtime, db, dispatchId, resource } = args
  const incarnation = resource.process_incarnation
  if (
    !incarnation ||
    (await runtime.inspectTerminalProcessIncarnationLiveness(incarnation, resource.host_scope)) !==
      'exited'
  ) {
    return null
  }
  // A handle can reappear while the owning provider is answering the death probe.
  const fresh = await inspectWorkerTerminal(runtime, db, dispatchId)
  if (
    (fresh.status !== 'missing' && fresh.status !== 'unattached') ||
    runtime.resolveTerminalHandleByProcessIncarnation(incarnation, resource.host_scope)
  ) {
    return null
  }
  const reconciled = db.settleDeadWorkerTerminalRelease({
    requestingDispatchId: dispatchId,
    resourceId: resource.id,
    processIncarnation: incarnation,
    expectedResource: resource
  })
  if (reconciled.disposition === 'released') {
    runtime.notifyMessageArrived(`dispatch:${dispatchId}`, 'status')
    return {
      dispatchId,
      state: 'released',
      processAction: 'none',
      archive: archiveSummary(reconciled.resource)
    }
  }
  const decision = decideWorkerTerminalRelease(reconciled.resource)
  return {
    dispatchId,
    state: decision.action === 'already_released' ? 'already_released' : 'retained',
    reason:
      decision.action === 'retained'
        ? decision.reason
        : reconciled.resource.retained_reason === 'user_requested'
          ? 'user_requested'
          : 'identity_unproven',
    processAction: 'none',
    archive: archiveSummary(reconciled.resource)
  }
}
