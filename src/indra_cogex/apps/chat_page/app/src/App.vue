<template>
  <h1>From App.vue</h1>
  <HelloWorld msg="Welcome to Your Vue.js App" />
</template>

<script>
import HelloWorld from "@/components/HelloWorld";

export default {
  name: "App",
  components: {
    HelloWorld,
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
