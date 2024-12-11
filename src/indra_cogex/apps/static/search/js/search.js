document.addEventListener('DOMContentLoaded', function () {
        const agentNameInput = document.getElementById('agent-name');
        const otherAgentInput = document.getElementById('other-agent-name');
        const otherAgentLabeltext = document.getElementById('other-agent-text');
        const roleButtons = document.querySelectorAll('.btn-role');
        const agentRoleInput = document.getElementById('agent-role');
        const otherAgentRoleInput = document.getElementById('other-agent-role');
        const otherAgentContainer = document.getElementById('other-agent-container');
        const agentSelect = document.getElementById('agent-select');
        const meshSelect = document.getElementById('mesh-select');
        const meshNameInput = document.getElementById('mesh-name');
        const stmtTypes = JSON.parse(document.getElementById('stmt-types-json').textContent);
        const selectElement = document.getElementById('choices-multiple-remove-button');
        const RelhiddenInput = document.getElementById('rel-type-hidden');
        const groundAgentButton = document.getElementById('ground-agent-button');
        const groundMeshButton = document.getElementById('ground-mesh-button');

        const exampleText = document.getElementById('clickable-text');

        const infoIcon = document.getElementById('info-icon');
        const tooltip = document.getElementById('tooltip');
//        const tooltipLink = document.getElementById('tooltip-link');





        groundAgentButton.addEventListener('click', async function () {
            const agentText = agentNameInput.value.trim();
            if (event.target.closest('.tooltip-box')) {
                return;
            }
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
//                const fixedWidth = "300px";
//                agentNameInput.style.width = fixedWidth;
//                agentSelect.style.width = fixedWidth;
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

                const role = this.dataset.role;

                // Update agent and other agent roles based on the role
                if (role === 'subject') {
                    otherAgentLabeltext.textContent = 'Other Agent (being object)';
                    agentRoleInput.value = 'subject';
                    otherAgentRoleInput.value = 'object';
                    otherAgentContainer.style.display = 'block';
                    otherAgentContainer.style.marginTop = '10px';
                } else if (role === 'object') {
                    otherAgentLabeltext.textContent = 'Other Agent (being subject)';
                    agentRoleInput.value = 'object';
                    otherAgentRoleInput.value = 'subject';
                    otherAgentContainer.style.display = 'block';
                    otherAgentContainer.style.marginTop = '10px';
                } else {
                    otherAgentLabeltext.textContent = 'Other Agent (being either subject or object)';
                    agentRoleInput.value = '';
                    otherAgentRoleInput.value = '';
                    otherAgentContainer.style.display = 'block';
                    otherAgentContainer.style.marginTop = '10px';
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
            RelhiddenInput.value = JSON.stringify(selectedValues); // Store as a JSON string
        });

        // Close the dropdown after selecting an option
        selectElement.addEventListener('choice', function () {
            choices.hideDropdown();
        });



        exampleText.addEventListener('click', function () {
            // Update agent and other agent
            agentNameInput.value = 'MEK';
            agentNameInput.readOnly = true;
            otherAgentInput.value = 'ERK';
            otherAgentInput.readOnly = true;

            // Update roles (select the second button)
            roleButtons.forEach(button => button.classList.remove('active'));
            const subjectButton = document.getElementById('btn-subject');
            subjectButton.classList.add('active');
            agentRoleInput.value = 'subject';
            otherAgentRoleInput.value = 'object';
            otherAgentContainer.style.display = 'block';
            otherAgentContainer.style.marginTop = '10px';
            // Update relationship type


        choices.setChoiceByValue('Phosphorylation');


        // Ensure the hidden input is updated with the selected value
        const selectedValues = ['Phosphorylation'];
        RelhiddenInput.value = JSON.stringify(selectedValues);

        });

        groundMeshButton.addEventListener('click', async function () {
            const meshText = meshNameInput.value.trim();
            if (event.target.closest('.tooltip-box')) {
                return;
            }
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

                const meshResults = data.filter(result => result.term.db === "MESH");
                if (meshResults.length === 0) {
                    alert("No Mesh grounding results found.");
                    return;
                }

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


        // Show/hide tooltip when the icon is clicked
        infoIcon.addEventListener('click', function (event) {
            event.stopPropagation(); // Prevent clicks from propagating
            if (tooltip.style.display === 'none' || tooltip.style.display === '') {
                tooltip.style.display = 'block';
            } else {
                tooltip.style.display = 'none';
            }
        });

//        tooltipLink.addEventListener('click', function (event) {
//            event.stopPropagation(); // Stops the event from propagating to the button
//        });

        // Hide the tooltip when clicking outside
        document.addEventListener('click', function (event) {
            if (!infoIcon.contains(event.target) && !tooltip.contains(event.target)) {
                tooltip.style.display = 'none';
            }
        });

        window.addEventListener('pageshow', function () {
            // Reset input box and hidden input
            agentNameInput.value = '';
            document.getElementById('agent-tuple').value = '';
            meshNameInput.value = '';
            document.getElementById('mesh-tuple').value = '';

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