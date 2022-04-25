<template>
  <p>Add navbar that link back to the main discovery app</p>
  <div class="container">
    <!-- Add horizontal line -->
    <hr />
    <DiscoveryApp />
  </div>
</template>

<script>
import DiscoveryApp from "@/components/DiscoveryApp.vue";

export default {
  name: "App",
  components: {
    DiscoveryApp,
  },
  data() {
    return {
      msg: "Welcome to Your Vue.js App",
    };
  },
};
</script>

<style>
#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}
</style>
