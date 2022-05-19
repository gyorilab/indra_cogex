<template>
  <div
    class="row"
    v-for="(stmt, index) in stmts"
    :key="`${this.componentUUID}-${index}`"
  >
    <span>{{ stmt.id }}</span>
  </div>
</template>

<script>
import helperFunctions from "@/helpers/helperFunctions";

export default {
  name: "StmtList.vue",
  props: {
    stmts: {
      /* Array of statements:
       * Each statement is an object with the following properties:
       *  - belief: Number
       *  - evidence: Array of evidence objects {
       *      - annotations: Object containing evidence annotations
       *      - epistemics: Object containing evidence epistemics
       *      - pmid: String of article PMID
       *      - source_api: String of source API name (e.g. "sparser", "reach", "biopax", "signor")
       *      - source_hash: Number of source hash (todo: this is larger than Number.MAX_SAFE_INTEGER, need to cast as string on server)
       *      - text: String of evidence text
       *      - text_refs: Object containing identifiers for different article sources, e.g. { "PMID": "12345", "doi": "10.1038/ncomms12456" }
       *    }
       *  - id: Hex String (what is this?)
       *  - matches_hash: String
       *  - obj: (optional) Object containing the statement object as an Agent: { name: "", db_refs: {...} }
       *  - obj_activity: (optional) String describing the activity of the object
       *  - subj: (optional) Object containing the statement subject as an Agent: { name: "", db_refs: {...} }
       *  - subj_activity: (optional) String describing the activity of the subject
       *  - type: String describing the statement type (e.g. "Activation", "Inhibition")
       */
      type: Array,
      required: true,
    },
  },
  computed: {
    componentUUID() {
      return `stmtList-${this.uuid}`;
    },
  },
  setup() {
    const uuid = helperFunctions.getUUID()();
    return {
      uuid,
    };
  },
};
</script>
