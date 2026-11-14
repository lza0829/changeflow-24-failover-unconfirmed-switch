<script setup>
import { computed, onMounted, ref } from 'vue'
import { getJSON, postJSON } from '../api'

const streams = ref([])
const streamId = ref(1)
const tasks = ref([])
const selected = ref(null)
const comparison = ref(null)
const badCursor = ref(0)
const phantomSeq = ref(99)
const err = ref('')
const busy = ref(false)

const currentStream = computed(() => streams.value.find((s) => s.id === streamId.value) || null)

async function refresh() {
  streams.value = await getJSON('/api/streams')
  tasks.value = await getJSON('/api/failover-rebuild/tasks')
  if (selected.value) {
    const id = selected.value.id
    selected.value = tasks.value.find((t) => t.id === id) || null
    comparison.value = selected.value
      ? await getJSON(`/api/failover-rebuild/tasks/${id}/comparison`)
      : null
  }
}

async function pickTask(t) {
  selected.value = t
  try {
    comparison.value = await getJSON(`/api/failover-rebuild/tasks/${t.id}/comparison`)
  } catch (e) {
    comparison.value = null
  }
}

async function act(fn) {
  busy.value = true
  err.value = ''
  try {
    await fn()
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

function startRebuild() {
  act(async () => {
    const task = await postJSON('/api/failover-rebuild/tasks', { stream_id: streamId.value })
    selected.value = task
    comparison.value = await getJSON(`/api/failover-rebuild/tasks/${task.id}/comparison`)
  })
}

function confirmTask() {
  act(async () => {
    await postJSON(`/api/failover-rebuild/tasks/${selected.value.id}/confirm`, {})
  })
}

function rejectTask() {
  act(async () => {
    await postJSON(`/api/failover-rebuild/tasks/${selected.value.id}/reject`, {
      reason: '页面人工拒绝',
    })
  })
}

function tamper() {
  act(() =>
    postJSON('/api/failover-rebuild/demo/tamper-cursor', {
      stream_id: streamId.value,
      bad_cursor: Number(badCursor.value),
    }),
  )
}

function injectConflict() {
  act(() =>
    postJSON('/api/failover-rebuild/demo/inject-sink-conflict', {
      stream_id: streamId.value,
      bad_cursor: Number(phantomSeq.value),
    }),
  )
}

function clearDemo() {
  act(() => postJSON('/api/failover-rebuild/demo/clear', { stream_id: streamId.value }))
}

function statusTag(s) {
  if (s === 'confirmed') return 'ok'
  if (s === 'awaiting_confirmation') return 'warn'
  return 'danger'
}

function statusText(s) {
  if (s === 'confirmed') return '已确认（已切换）'
  if (s === 'awaiting_confirmation') return '待确认（游标锁定）'
  if (s === 'rejected_manual') return '已拒绝（人工，游标未动）'
  if (s === 'rejected') return '已拒绝（门禁，游标未动）'
  return s
}

function phaseEvents(task) {
  const events = []
  if (task?.created_at) {
    events.push({
      phase: 'proposal',
      at: task.created_at,
      title: task.adoptable ? '创建任务 · 候选提案' : '创建任务 · 门禁拒绝',
      detail: {
        游标前: task.cursor_before,
        候选: task.candidate_cursor,
        gate: task.gate,
        证据指纹: task.evidence_fingerprint,
      },
    })
  }
  const decision = task?.audit?.decision
  if (task?.decided_at || decision) {
    const action = decision?.action
    events.push({
      phase: 'decision',
      at: task.decided_at || '同一事务',
      title:
        action === 'confirm'
          ? '人工确认 · 切换游标'
          : decision?.action_by === 'gate' || task?.status === 'rejected'
            ? '门禁拒绝 · 游标不动'
            : '人工拒绝 · 游标不动',
      detail: {
        动作: action || '—',
        游标后: task.cursor_after,
        未改动: String(decision?.untouched ?? task?.cursor_switched === false),
      },
    })
  }
  return events
}

const auditTimeline = computed(() => phaseEvents(selected.value))

// 三步验收演练：未确认锁定 → 矛盾拒绝不动游标 → 确认切换
const drill = computed(() => {
  const id = streamId.value
  const stream = currentStream.value
  const mine = tasks.value.filter((t) => t.stream_id === id)
  const pending = mine.find((t) => t.status === 'awaiting_confirmation')
  const rejected = mine.find(
    (t) => t.status === 'rejected' || t.status === 'rejected_manual',
  )
  const confirmed = mine.find((t) => t.status === 'confirmed')
  return [
    {
      key: 'pending',
      label: '发起重建但不确认：游标应保持任务前值',
      done: !!pending && (stream?.apply_cursor ?? -1) === pending.cursor_before,
      hint: pending
        ? `任务 #${pending.id}：游标 ${stream?.apply_cursor} = 任务前 ${pending.cursor_before}`
        : '尚无待确认任务',
    },
    {
      key: 'reject',
      label: '注入矛盾证据后拒绝：游标不得切到任一侧',
      done:
        !!rejected &&
        rejected.cursor_after === rejected.cursor_before &&
        (stream?.apply_cursor ?? -1) === rejected.cursor_before,
      hint: rejected
        ? `任务 #${rejected.id}：游标 ${stream?.apply_cursor} 未动（前 ${rejected.cursor_before}）`
        : '尚无被拒绝任务',
    },
    {
      key: 'confirm',
      label: '走通一次确认：游标切到候选位点',
      done:
        !!confirmed &&
        confirmed.cursor_after === confirmed.candidate_cursor &&
        (stream?.apply_cursor ?? -1) === confirmed.cursor_after,
      hint: confirmed
        ? `任务 #${confirmed.id}：游标 ${confirmed.cursor_after} = 候选 ${confirmed.candidate_cursor}`
        : '尚无已确认任务',
    },
  ]
})

onMounted(refresh)
</script>

<template>
  <div class="page">
    <h1>故障位点重建</h1>
    <p class="muted">
      apply_cursor 丢失/改坏后，以 Sink 与已应用账本为证据重建候选位点，人工确认后才切换。
      未确认前<b>禁止改写流游标</b>；证据不足或矛盾必须拒绝切换且游标保持任务前值。
      不是回拨重放，也不是行级校验/漂移门禁。可对照
      <router-link to="/ledger">变更账本</router-link>、
      <router-link to="/sink">Sink</router-link>、
      <router-link to="/cursors">游标</router-link> 页核验。
    </p>

    <div class="row-actions panel">
      <label>流
        <select v-model.number="streamId">
          <option v-for="s in streams" :key="s.id" :value="s.id">
            {{ s.id }} · {{ s.name }}（当前游标 {{ s.apply_cursor }}）
          </option>
        </select>
      </label>
      <span class="tag ok mono">当前游标：{{ currentStream?.apply_cursor ?? '—' }}</span>
      <button :disabled="busy" @click="startRebuild">{{ busy ? '扫描中…' : '发起重建（扫证据，不改游标）' }}</button>
    </div>

    <div class="panel">
      <h3>演练 / 验收工具（故障注入，非主链）</h3>
      <div class="row-actions">
        <label>坏游标值 <input v-model="badCursor" type="number" style="width: 6rem" /></label>
        <button class="ghost" :disabled="busy" @click="tamper">改坏游标</button>
        <label>幻影 Sink seq <input v-model="phantomSeq" type="number" style="width: 6rem" /></label>
        <button class="ghost" :disabled="busy" @click="injectConflict">注入 Sink/账本矛盾</button>
        <button class="ghost" :disabled="busy" @click="clearDemo">清理注入数据</button>
      </div>
    </div>

    <div class="panel">
      <h3>验收走查（当前流 {{ streamId }}）</h3>
      <ul style="font-size: 0.9rem">
        <li v-for="d in drill" :key="d.key">
          <span class="tag" :class="d.done ? 'ok' : 'warn'">{{ d.done ? '✓' : '待' }}</span>
          {{ d.label }}
          <div class="muted mono" style="font-size: 0.8rem">{{ d.hint }}</div>
        </li>
      </ul>
    </div>

    <p v-if="err" class="tag danger">{{ err }}</p>

    <div class="panel" v-if="selected">
      <h3>
        重建任务 #{{ selected.id }}
        <span class="tag" :class="statusTag(selected.status)">{{ statusText(selected.status) }}</span>
        <span class="tag warn">gate: {{ selected.gate }}</span>
      </h3>

      <p v-if="selected.status === 'awaiting_confirmation'" class="tag warn">
        游标锁定 = 未切换：当前游标 {{ selected.live_cursor }} = 任务前值 {{ selected.cursor_before }}，
        候选 {{ selected.candidate_cursor }} 仅在任务记录中，确认后才写入。
      </p>
      <p v-else-if="selected.status === 'confirmed'" class="tag ok">
        已确认切换：{{ selected.cursor_before }} → {{ selected.cursor_after }}，当前游标 {{ selected.live_cursor }}。
      </p>
      <p v-else class="tag danger">
        已拒绝：游标保持任务前值 {{ selected.cursor_before }}（当前 {{ selected.live_cursor }}），未切到任一侧。
      </p>
      <p v-if="selected.status === 'awaiting_confirmation' && !selected.cursor_locked" class="tag danger">
        异常：待确认任务的游标已偏离任务前值（可能被外部改写），确认将被拒绝。
      </p>

      <table>
        <tbody>
          <tr><th>流</th><td>{{ selected.stream_id }}</td></tr>
          <tr><th>候选位点</th><td><b>{{ selected.candidate_cursor }}</b>（adoptable: {{ String(selected.adoptable) }}）</td></tr>
          <tr><th>任务前游标</th><td>{{ selected.cursor_before }}</td></tr>
          <tr><th>决策后游标</th><td>{{ selected.cursor_after }}</td></tr>
          <tr><th>当前流游标</th><td>{{ selected.live_cursor }} · 锁定={{ String(selected.cursor_locked) }} · 已切换={{ String(selected.cursor_switched) }}</td></tr>
          <tr><th>证据指纹</th><td class="mono">{{ selected.evidence_fingerprint }}</td></tr>
          <tr><th>创建 / 决策时间</th><td>{{ selected.created_at }} / {{ selected.decided_at || '—' }}</td></tr>
        </tbody>
      </table>

      <h4>证据对照（ledger 侧 vs sink 侧，只读接口同源）</h4>
      <p v-if="comparison">
        <span class="tag" :class="comparison.sufficient ? 'ok' : 'danger'">
          {{ comparison.sufficient ? '证据充分' : '证据不足（G1）' }}
        </span>
        <span class="tag" :class="comparison.conflict ? 'danger' : 'ok'">
          {{ comparison.conflict ? '两侧证据冲突：拒绝切换' : '两侧证据一致' }}
        </span>
        <span class="tag" :class="comparison.evidence_changed ? 'warn' : 'ok'">
          {{ comparison.evidence_changed ? '证据已相对任务快照变化' : '证据与任务快照一致' }}
        </span>
      </p>
      <table v-if="comparison">
        <thead><tr><th>证据侧</th><th>候选分</th><th>说明</th></tr></thead>
        <tbody>
          <tr>
            <td>ledger 侧</td>
            <td>{{ comparison.ledger_side.candidate_score }}</td>
            <td class="muted">
              applied {{ comparison.ledger_side.applied_count }} 条 · change_ledger
              {{ comparison.ledger_side.ledger_applied_count }} 条 · 两来源一致
              {{ String(comparison.ledger_side.sources_agree) }}
            </td>
          </tr>
          <tr>
            <td>sink 侧</td>
            <td>{{ comparison.sink_side.candidate_score }}</td>
            <td class="muted">
              Sink 行 {{ comparison.sink_side.record_count }} · 越账本
              {{ comparison.sink_side.ahead_of_ledger_by }} · 未见账本 seq
              <span class="mono">{{ comparison.sink_side.unexplained_seqs.join(',') || '无' }}</span>
            </td>
          </tr>
        </tbody>
      </table>
      <ul v-if="comparison?.conflict_reasons?.length" style="color: var(--danger); font-size: 0.9rem">
        <li v-for="(r, i) in comparison.conflict_reasons" :key="i">{{ r }}</li>
      </ul>

      <h4>扫描规模</h4>
      <table>
        <thead><tr><th>证据源</th><th>扫描行数</th></tr></thead>
        <tbody>
          <tr v-for="(v, k) in selected.scan_counts" :key="k">
            <td>{{ k }}</td><td>{{ v }}</td>
          </tr>
        </tbody>
      </table>

      <h4>证据摘要</h4>
      <table>
        <tbody>
          <tr><th>applied_seqs</th><td class="mono">{{ selected.evidence.applied_seqs.join(', ') || '（空）' }}</td></tr>
          <tr><th>sink last_seq</th><td class="mono">{{ selected.evidence.sink_last_seqs.join(', ') || '（空）' }}</td></tr>
          <tr><th>quarantine seq</th><td class="mono">{{ selected.evidence.quarantine_seqs.join(', ') || '（空）' }}</td></tr>
          <tr><th>计数</th><td class="muted">applied {{ selected.evidence.applied_count }} · sink {{ selected.evidence.sink_record_count }} · 隔离 {{ selected.evidence.quarantine_count }} · ledger {{ selected.evidence.ledger_applied_count }}</td></tr>
        </tbody>
      </table>

      <h4>计算依据</h4>
      <ul class="mono" style="font-size:0.85rem">
        <li v-for="(b, i) in selected.basis" :key="i">{{ b }}</li>
      </ul>

      <div v-if="selected.reasons.length">
        <h4>拒绝原因</h4>
        <ul style="color: var(--danger); font-size: 0.9rem">
          <li v-for="(r, i) in selected.reasons" :key="i">{{ r }}</li>
        </ul>
      </div>

      <h4>双相审计时间线（proposal → decision）</h4>
      <table>
        <tbody>
          <tr v-for="(e, i) in auditTimeline" :key="i">
            <td><span class="tag" :class="e.phase === 'decision' ? 'ok' : 'warn'">{{ e.phase }}</span></td>
            <td>{{ e.at }}</td>
            <td>{{ e.title }}</td>
            <td class="mono muted" style="font-size:0.8rem">
              {{ Object.entries(e.detail).map(([k, v]) => `${k}=${v}`).join(' · ') }}
            </td>
          </tr>
        </tbody>
      </table>
      <details>
        <summary class="muted">原始审计 JSON</summary>
        <pre class="mono">{{ JSON.stringify(selected.audit, null, 2) }}</pre>
      </details>

      <div class="row-actions" v-if="selected.status === 'awaiting_confirmation'">
        <button :disabled="busy" @click="confirmTask">确认采纳并切换游标</button>
        <button class="ghost" :disabled="busy" @click="rejectTask">拒绝（游标不动）</button>
      </div>
      <p v-else class="muted">任务已终态，不可再决策。</p>
    </div>

    <div class="panel">
      <h3>重建任务记录（持久化）</h3>
      <table>
        <thead>
          <tr><th>ID</th><th>流</th><th>状态</th><th>gate</th><th>候选</th><th>采纳</th><th>游标前→后</th><th>锁定/切换</th><th>时间</th><th></th></tr>
        </thead>
        <tbody>
          <tr v-for="t in tasks" :key="t.id">
            <td>{{ t.id }}</td>
            <td>{{ t.stream_id }}</td>
            <td><span class="tag" :class="statusTag(t.status)">{{ statusText(t.status) }}</span></td>
            <td>{{ t.gate }}</td>
            <td>{{ t.candidate_cursor }}</td>
            <td>{{ String(t.adoptable) }}</td>
            <td>{{ t.cursor_before }} → {{ t.cursor_after }}</td>
            <td class="muted">锁定={{ String(t.cursor_locked) }} · 切换={{ String(t.cursor_switched) }}</td>
            <td>{{ t.created_at }}</td>
            <td><a href="#" @click.prevent="pickTask(t)">查看证据</a></td>
          </tr>
          <tr v-if="!tasks.length"><td colspan="10" class="muted">暂无重建任务</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
