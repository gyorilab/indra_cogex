<template>
  <div class="col">
    <span class="text-muted small" :title="receivedDate">{{ shortDate }}</span
    >&nbsp;
    <span v-html="message.text"></span>
  </div>
</template>

<script>
export default {
  name: "MessageWrapper.vue",
  data() {
    return {
      listMax: 0,
    };
  },
  props: {
    message: {
      type: Object,
      required: true,
    },
  },
  computed: {
    receivedDate() {
      return new Date(this.message.receivedAt).toLocaleString();
    },
    shortDate() {
      // Format: "HH:mm" 24 hour clock
      return new Date(this.message.receivedAt).toLocaleTimeString("en-GB", {
        hour: "2-digit",
        minute: "2-digit",
      });
    },
  },
};
</script>

<style scoped></style>
