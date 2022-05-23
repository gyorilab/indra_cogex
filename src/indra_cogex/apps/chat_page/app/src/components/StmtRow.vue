<template>
  <div class="col-auto">
    <span>{{ stmt.english }}</span>
    Agents:
    <span
      v-for="(ag, innerIndex) in agentList"
      :key="`${this.componentUUID}-${index}-${innerIndex}`"
      ><AgentModal :agent-object="ag"
    /></span>
  </div>
  <div class="col text-end">
    Full statement info:
    <a
      :href="`https://discovery.indra.bio/statement_display?stmt_hash=${stmt.matches_hash}`"
      target="_blank"
      rel="noopener noreferrer"
      ><i class="bi bi-box-arrow-up-right"></i
    ></a>
  </div>
</template>

<script>
import AgentModal from "@/components/AgentModal.vue";

export default {
  name: "StmtRow.vue",
  components: {
    AgentModal,
  },
  props: {
    stmt: {
      type: Object,
      required: true,
    },
    index: {
      type: Number,
      required: true,
    },
  },
  computed: {
    agentList() {
      return this.$parent.getAgentList(this.stmt);
    },
    componentUUID() {
      return this.stmt.id;
    },
  },
};
</script>

<style scoped></style>
