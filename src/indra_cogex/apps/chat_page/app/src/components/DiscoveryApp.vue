<template>
  <p>This is the app key: {{ app_key }}</p>
</template>

<script>
export default {
  name: "DiscoveryApp.vue",
  data() {
    return {
      app_key: "",
    };
  },
  mounted() {
    this.getAppKey();
  },
  methods: {
    async getAppKey() {
      const resp = await fetch("/chat/pusher_info");
      const pusher_info = await resp.json();
      this.app_key = pusher_info.pusher_app_key;
    },
  },
};
</script>

<style scoped></style>
