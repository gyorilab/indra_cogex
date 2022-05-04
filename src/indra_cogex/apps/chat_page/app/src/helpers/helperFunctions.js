/**
 * Helper functions for the chat page
 */

/* Get globally unique IDs */
let UUID = 0;
export default function getUUID() {
  return () => {
    UUID++;
    return UUID;
  };
}
