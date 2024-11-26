document.addEventListener('DOMContentLoaded', function () {
        const agentNameInput = document.getElementById('agent-name');
        const otherAgentInput = document.getElementById('other-agent-name');
        const roleButtons = document.querySelectorAll('.btn-role');
        const agentRoleInput = document.getElementById('agent-role');
        const otherAgentRoleInput = document.getElementById('other-agent-role');
        const otherAgentContainer = document.getElementById('other-agent-container');
        const agentSelect = document.getElementById('agent-select');
        const meshSelect = document.getElementById('mesh-select');

        const meshNameInput = document.getElementById('mesh-name');




        const stmtTypes = JSON.parse(document.getElementById('stmt-types-json').textContent);
        console.log('stmt-types-json element:', stmtTypes);
        const selectElement = document.getElementById('choices-multiple-remove-button');
        const hiddenInput = document.getElementById('rel-type-hidden');
        const groundAgentButton = document.getElementById('ground-agent-button');
        const groundMeshButton = document.getElementById('ground-mesh-button');



    groundAgentButton.addEventListener('click', async function () {
        const agentText = agentNameInput.value.trim();
        if (!agentText) {
            alert("Please enter an agent name to ground.");
            return;
        }
        try {
            const response = await fetch('/search/gilda_ground', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ agent: agentText }),
            });
            if (!response.ok) throw new Error('Failed to ground the agent.');
            const data = await response.json();

            if (!data || data.length === 0) {
                alert("No grounding results found.");
                return;
            }
            agentSelect.innerHTML = '';
            const placeholderOption = document.createElement('option');
            placeholderOption.textContent = 'Grounded Results...';
            placeholderOption.value = '';
            placeholderOption.hidden = true; // non-selectable
            placeholderOption.selected = true; // Show as default selected
            agentSelect.appendChild(placeholderOption);
            data.forEach(result => {
                const option = document.createElement('option');
                option.value = JSON.stringify({
                    source_db: result.term.db,
                    source_id: result.term.id,
                });
                option.textContent = `${result.term.entry_name} (${result.term.db}, Score: ${result.score.toFixed(2)})`;
                agentSelect.appendChild(option);
            });
            agentNameInput.style.display = 'none';
            agentSelect.style.display = 'block';
        } catch (error) {
            console.error("Error grounding agent:", error);
            alert("An error occurred while grounding the agent.");
        }
    });

        agentSelect.addEventListener('change', function () {
            const selectedOption = agentSelect.options[agentSelect.selectedIndex];
            if (selectedOption) {
                const { source_db, source_id } = JSON.parse(selectedOption.value);

                // Update the display text in the input box
                agentNameInput.value = selectedOption.textContent;
                agentNameInput.readOnly = true;

                document.getElementById('agent-tuple').value = JSON.stringify([source_db, source_id]);
            }
        });

        // Role Button Click Handlers
        roleButtons.forEach(button => {
            button.addEventListener('click', function () {
                // Remove 'active' class from all buttons
                roleButtons.forEach(btn => btn.classList.remove('active'));

                // Add 'active' class to the clicked button
                this.classList.add('active');

                // Determine role based on the clicked button
                const role = this.dataset.role;

                // Update agent and other agent roles based on the role
                if (role === 'subject') {
                    agentRoleInput.value = 'subject';
                    otherAgentRoleInput.value = 'object';
                    otherAgentContainer.style.display = 'block';
                    otherAgentContainer.style.marginTop = '10px';
                } else if (role === 'object') {
                    agentRoleInput.value = 'object';
                    otherAgentRoleInput.value = 'subject';
                    otherAgentContainer.style.display = 'block';
                    otherAgentContainer.style.marginTop = '10px';
                } else {
                    agentRoleInput.value = '';
                    otherAgentRoleInput.value = '';
                }

            });
        });
        // Initialize Choices.js
        const choices = new Choices(selectElement, {
            removeItemButton: true,
            searchResultLimit: stmtTypes.length,
            renderChoiceLimit: stmtTypes.length,
        });

        // Add options to Choices.js dynamically
        stmtTypes.forEach(type => {
            choices.setChoices([{ value: type, label: type, selected: false }], 'value', 'label', false);
        });

        // Update the hidden input whenever selections change
        selectElement.addEventListener('change', function () {
            const selectedValues = Array.from(selectElement.selectedOptions).map(option => option.value);
            hiddenInput.value = JSON.stringify(selectedValues); // Store as a JSON string
        });

        // Close the dropdown after selecting an option
        selectElement.addEventListener('choice', function () {
            choices.hideDropdown();
        });




        groundMeshButton.addEventListener('click', async function () {
            const meshText = meshNameInput.value.trim();
            if (!meshText) {
                alert("Please enter a Mesh name to ground.");
                return;
            }
            try {
                const response = await fetch('/search/gilda_ground', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({ agent: meshText }),
                });
                if (!response.ok) throw new Error('Failed to ground the Mesh.');
                const data = await response.json();

                if (!data || data.length === 0) {
                    alert("No grounding results found.");
                    return;
                }

                // Filter results to include only those where result.term.db === "MESH"
                const meshResults = data.filter(result => result.term.db === "MESH");
                if (meshResults.length === 0) {
                    alert("No Mesh grounding results found.");
                    return;
                }

                // Populate dropdown with filtered results
                meshSelect.innerHTML = '';
                const placeholderOption = document.createElement('option');
                placeholderOption.textContent = 'Grounded Results...';
                placeholderOption.value = '';
                placeholderOption.hidden = true; // non-selectable
                placeholderOption.selected = true; // Show as default selected
                meshSelect.appendChild(placeholderOption);

                meshResults.forEach(result => {
                    const option = document.createElement('option');
                    option.value = JSON.stringify({
                        source_db: result.term.db,
                        source_id: result.term.id,
                    });
                    option.textContent = `${result.term.entry_name} (${result.term.db}, Score: ${result.score.toFixed(2)})`;
                    meshSelect.appendChild(option);
                });

                // Show the dropdown
                meshNameInput.style.display = 'none';
                meshSelect.style.display = 'block';
            } catch (error) {
                console.error("Error grounding Mesh:", error);
                alert("An error occurred while grounding the Mesh.");
            }
        });

        meshSelect.addEventListener('change', function () {
            const selectedOption = meshSelect.options[meshSelect.selectedIndex];
            if (selectedOption) {
                const { source_db, source_id } = JSON.parse(selectedOption.value);

                meshNameInput.value = selectedOption.textContent;
                meshNameInput.readOnly = true;

                document.getElementById('mesh-tuple').value = JSON.stringify([source_db, source_id]);
            }
        });

        window.addEventListener('pageshow', function () {
            // Reload the page when navigating back
            const agentNameInput = document.getElementById('agent-name');
            const agentSelect = document.getElementById('agent-select');
            const hiddenInput = document.getElementById('agent-tuple');

            // Reset input box and hidden input
            agentNameInput.value = '';
            hiddenInput.value = '';

            // Clear the dropdown and add placeholder option
            agentSelect.innerHTML = '';
            const placeholderOption = document.createElement('option');
            placeholderOption.textContent = 'Grounding Result';
            placeholderOption.value = '';
            placeholderOption.hidden = true;
            placeholderOption.selected = true;
            agentSelect.appendChild(placeholderOption);
        });
    });