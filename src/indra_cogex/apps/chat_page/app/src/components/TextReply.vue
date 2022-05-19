<template>
  <!-- Show spinner if receivedDate is null -->
  <div v-if="!receivedDate" class="d-flex align-items-center">
    <span>Loading...</span>
    <div
      class="spinner-grow spinner-grow-sm text-secondary ms-auto"
      role="status"
      aria-hidden="true"
    ></div>
  </div>
  <template v-if="raw_text">
    <p class="text-start">
      <template
        v-for="(txtObj, index) in textObjectArray"
        :key="`${componentID}-text${index}`"
      >
        <!-- No object: just display the text as innerHtml -->
        <span v-if="txtObj.object === null" v-html="txtObj.text"></span>
        <!-- A single agent from query: display as AgentModal -->
        <template
          v-else-if="
            txtObj.object.role === 'fixed' && txtObj.object.type === 'agent'
          "
        >
          <AgentModal :agent-object="txtObj.object.value" />
        </template>
        <!-- A string list, set as an unordered list -->
        <ul v-else-if="txtObj.object.type === 'string_list'">
          <li
            v-for="(str, index) in txtObj.object.value"
            :key="`${componentID}-strlist${index}`"
          >
            {{ str }}
          </li>
        </ul>
        <!-- agent list with reply entities: display the computed string -->
        <span
          v-else-if="
            txtObj.object.type === 'agent_list' &&
            txtObj.object.role === 'variable'
          "
          v-html="txtObj.text"
        ></span>
        <!-- URL list -->
        <span v-else-if="txtObj.object.type === 'url_list'">
          <span
            v-for="([url, db_name], index) in txtObj.object.value"
            :key="`${componentID}-urllist${index}`"
          >
            <a :href="url" target="_blank">{{ db_name }}</a>
            {{ index === txtObj.object.value.length - 2 ? "and " : ", " }}
          </span>
        </span>
        <!-- Clarification: display the computed string -->
        <div v-if="clarification">
          <button
            class="btn btn-sm btn-outline-secondary"
            @click="$emit('clarification-requested1', clarification)"
          >
            Copy clarification to input
          </button>
        </div>
      </template>
    </p>
  </template>
</template>

<script>
import AgentModal from "@/components/AgentModal.vue";
import helperFunctions from "@/helpers/helperFunctions";

export default {
  name: "TextReply.vue",
  emits: ["clarification-requested1"],
  components: {
    AgentModal,
  },
  props: {
    raw_text: {
      type: String,
      default: "",
    },
    objects: {
      type: Object,
      default: null,
    },
    receivedDate: {
      type: String,
      default: "",
    },
  },
  computed: {
    clarification() {
      // Match 'Your question is similar to "%s". Try asking it that way.'
      const clarificationRegex =
        /Your question is similar to "(.*)". Try asking it that way./;
      const match = clarificationRegex.exec(this.raw_text);
      if (match) {
        return match[1];
      }
      return "";
    },
    componentID() {
      return `text-reply-${this.uuid}`;
    },
    textObjectArray() {
      // Locate all references to text objects in raw text

      // Contains objects with entries: "text", "object"
      // "text" is the text to show, "object" is the object representing the text (null if plain text)
      let txtObjs = [];
      const rawText = this.raw_text || "";

      // Split string on any text between '{' and '}'
      let txtObjStrings = rawText.split(/(\{.*?\})/g);

      if (txtObjStrings.length > 0 && this.objects !== null) {
        // Loop through each string and match against objects
        txtObjStrings.forEach((substring) => {
          // Skip empty strings
          if (substring.length > 0) {
            // If we match a recognised object, look it up among the objects
            if (substring.includes("{") && substring.includes("}")) {
              let objName = substring.replace(/[{}]/g, "");
              let limit;
              // Check if there is a limit
              if (objName.includes(":")) {
                [objName, limit] = objName.split(":");
              } else {
                limit = null;
              }
              const obj = this.objects[objName] || null;
              let textForObj;
              if (obj) {
                let formatSpec;
                if (limit !== null) {
                  // Get the numeric value of the limit, e.g. "limit10" -> 10
                  limit = parseInt(limit.replace(/limit/g, ""), 10);
                  formatSpec = { limit: limit };
                }
                textForObj = this.getTextForObj(obj, formatSpec);
                txtObjs.push({
                  text: textForObj,
                  object: obj,
                  objName: objName,
                  match: substring, // The string that matched the object
                });
              }
            } else {
              // If we don't match a recognised object, just add the string
              txtObjs.push({
                text: substring,
                object: null,
                objName: null,
                match: substring,
              });
            }
          }
        });
      } else {
        console.log("Shouldn't get here");
        if (rawText.includes("{") || rawText.includes("}")) {
          console.warn(
            "MessageWrapper.vue: No text objects found in raw text, but references found. Returning raw text."
          );
        } else {
          console.log("No text objects found");
        }
        txtObjs.push({
          text: this.raw_text,
          object: null,
          objName: null,
          match: null,
        });
      }
      return txtObjs;
    },
  },
  methods: {
    englishJoin(arr, delimiter = ", ", oxford = true) {
      // Join an array of strings into a string with a delimiter and a grammatically correct conjunction
      if (arr.length === 0) {
        console.warn("No array to join", arr);
      } else if (arr.length === 1) {
        return arr[0];
      } else if (arr.length === 2) {
        return `${arr[0]} and ${arr[1]}`;
      } else {
        return `${arr.slice(0, -1).join(delimiter)}${
          oxford ? "," : ""
        } and ${arr.slice(-1)}`;
      }
    },
    getTextForObj(obj, format_spec = null) {
      // Return the text for the object
      switch (obj.type) {
        // Todo: handle 'stmt_list', 'url', 'url_list', 'string_list', 'str'
        case "agent":
          // Return object.value.name
          return obj.value.name;
        case "agent_list": {
          // If there is a limit, return the first N agent names as a string
          let limit;
          if (format_spec) {
            limit = format_spec.limit;
          }
          limit = limit || obj.value.length;
          let nameArray = obj.value.slice(0, limit).map((agent) => agent.name);
          return this.englishJoin(nameArray);
        }
        case "string_list":
        case "url_list":
          /* string_list, url_list is an arrays of strings that are joined together with
           * a delimiter in the template and doesn't need any text */
          return null;
        case "string":
          // Return the string
          return obj.value;
        default:
          console.log("In switch-case; unhandled object type: " + obj.type);
          console.log(obj);
          return "(N/A)";
      }
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
