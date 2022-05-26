# Setting up the chat app for usage with imported components from indralab-vue

## Packaging `indralab-vue` for local install
In the root of `indralab-vue`:

- Run `npm run build`; this will create or update the `dist` directory (`dist` is the default name, other names might 
  be set)
- Run `npm run pack`; this will package the project in tgz file that can be used as a standalone installation

## Installing indralab-vue locally

In the root directory of the Vue chat app, i.e. `indra_cogex/apps/chat_page/app`, install the `indralab-vue` from the 
.tgz file:

```sh
npm install /path/to/indralab-vue/indralab-vue-0.0.1.tgz
```

_Note that the version number might be different._

## Importing components from `indralab-vue`

It seems that when components from the library are nested, the nested/child components are not resolved. It's unclear 
why, but a workaround is to register all the necessary components globally in the app setup in `main.js`:

```js
// indra_cogex/src/indra_cogex/apps/chat_page/app/src/main.js
import { createApp } from "vue";

import { Statement } from "indralab-vue";
import { Evidence } from "indralab-vue";
import { CurationRow } from "indralab-vue";
import { RefLink } from "indralab-vue";
import { SourceDisplay } from "indralab-vue";
import App from "./App.vue";

// ... other setup options

const app = createApp(App);

app.component("Statement", Statement);
app.component("Evidence", Evidence);
app.component("RefLink", RefLink);
app.component("SourceDisplay", SourceDisplay);
app.component("CurationRow", CurationRow);


app.mount("#app");
```
