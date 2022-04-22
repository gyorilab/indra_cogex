<template>
  <div class="col">
    <p class="text-muted small" :title="receivedDate">{{ shortDate }}</p>
    <p class="col">{{ initialText }}</p>
    <ul>
      <li v-for="(linkHtml, index) in htmlList.slice(0, listMax)" :key="index">
        <span v-html="linkHtml"></span>
      </li>
    </ul>
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
    // The maximum number of items to show initially in a rendered list.
    showMax: {
      type: Number,
      default: 10,
    },
    listIncrement: {
      type: Number,
      default: 10,
    },
  },
  mounted() {
    // Set the maximum number of items to show initially.
    this.listMax = this.showMax;
  },
  methods: {
    increaseListMax() {
      // Increase the maximum number of items to show.
      this.listMax += this.listIncrement;
    },
    showAll() {
      // Show all items in the list.
      this.listMax = this.message.htmlList.length;
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
    initialText() {
      // Split on ":", then take the first element
      return this.message.text.split(":")[0];
    },
    htmlList() {
      // Split on ":", then take the second element. Then split on "," and take all but the last element
      let first_n_1 = this.message.text.split(":")[1].split(",").slice(0, -1);
      // Take the last element and remove "and"
      let last = this.message.text
        .split(":")[1]
        .split(",")
        .slice(-1)[0]
        .replace("and", "");
      return [...first_n_1, last];
    },
  },
};
</script>

<style scoped></style>
