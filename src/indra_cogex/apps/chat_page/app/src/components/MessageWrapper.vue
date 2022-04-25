<template>
  <div class="col card">
    <div class="card-body">
      <div class="row">
        <div class="col-md-2">
          <span class="text-muted small" :title="receivedDate">{{
            shortDate
          }}</span>
        </div>
        <div class="col-auto text-left">
          <span v-html="message.text"></span>
        </div>
      </div>
    </div>
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
