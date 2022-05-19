<template>
  <p>Statement List with {{ stmts.length }} statements</p>
</template>

<script>
export default {
  name: "StmtList.vue",
  props: {
    stmts: {
      type: Array,
      required: true,
    },
  },
};
</script>
