<template>
  <div class="hello">
    <h2>{{ msg }}</h2>
    <p>From HelloWorld.vue</p>
    <p>
      For a guide and recipes on how to configure / customize this project,<br />
      check out the
      <a href="https://cli.vuejs.org" target="_blank" rel="noopener"
        >vue-cli documentation</a
      >.
    </p>
  </div>
  <div class="container">
    <DiscoveryApp />
  </div>
</template>

<script>
import DiscoveryApp from "@/components/DiscoveryApp.vue";

export default {
  name: "HelloWorld",
  components: {
    DiscoveryApp,
  },
  props: {
    msg: String,
  },
};
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
h3 {
  margin: 40px 0 0;
}

ul {
  list-style-type: none;
  padding: 0;
}

li {
  display: inline-block;
  margin: 0 10px;
}

a {
  color: #42b983;
}
</style>
