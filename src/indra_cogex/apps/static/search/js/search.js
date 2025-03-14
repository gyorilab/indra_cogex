document.addEventListener('DOMContentLoaded', function () {
        const agentNameInput = document.getElementById('agent-name');
        const agentDisplay = document.getElementById('agent-display');
        const otherAgentDisplay = document.getElementById('other-agent-display');
        const meshDisplay = document.getElementById('mesh-display');

        const otherAgentInput = document.getElementById('other-agent-name');
        const otherAgentLabeltext = document.getElementById('other-agent-text');
        const roleButtons = document.querySelectorAll('.btn-role');
        const agentRoleInput = document.getElementById('agent-role');
        const otherAgentRoleInput = document.getElementById('other-agent-role');
        const otherAgentContainer = document.getElementById('other-agent-container');
        const agentSelect = document.getElementById('agent-select');
        const otherAgentSelect = document.getElementById('other-agent-select');
        const meshSelect = document.getElementById('mesh-select');
        const meshNameInput = document.getElementById('mesh-name');
        const stmtTypes = JSON.parse(document.getElementById('stmt-types-json').textContent);
        const selectElement = document.getElementById('choices-multiple-remove-button');
        const RelhiddenInput = document.getElementById('rel-type-hidden');
        const groundAgentButton = document.getElementById('ground-agent-button');
        const cancelAgentButton = document.getElementById('cancel-agent-button');
        const cancelOtherAgentButton = document.getElementById('cancel-other-agent-button');
        const cancelMeshButton = document.getElementById('cancel-mesh-button');
        const groundOtherAgentButton = document.getElementById('ground-other-agent-button');

        const groundMeshButton = document.getElementById('ground-mesh-button');

        const exampleText1 = document.getElementById('clickable-text-example1');
        const exampleText2 = document.getElementById('clickable-text-example2');
        const exampleText3 = document.getElementById('clickable-text-example3');
        const exampleText4 = document.getElementById('clickable-text-example4');
        const exampleText5 = document.getElementById('clickable-text-example5');

        const infoIcon = document.getElementById('info-icon');
        const tooltip = document.getElementById('tooltip');


        // First button clicked by default
        const firstButton = roleButtons[0];
         if (firstButton) {
        firstButton.classList.add('active');
         otherAgentLabeltext.textContent = 'Other Agent';
                    agentRoleInput.value = '';
                    otherAgentRoleInput.value = '';
                    otherAgentContainer.style.display = 'block';
                    otherAgentContainer.style.marginTop = '10px';
        firstButton.click();
        }

        async function groundEntity(inputElement, displayElement, selectElement, groundButton, cancelButton, tupleId, entityType) {
            const entityText = inputElement.value.trim();
            if (!entityText) {
                alert(`Please enter a ${entityType} name to ground.`);
                return;
            }

            try {
                const response = await fetch('/search/gilda_ground', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ agent: entityText }),
                });

                if (!response.ok) throw new Error(`Failed to ground the ${entityType}.`);
                const data = await response.json();

                if (!data || data.length === 0) {
                    alert(`No grounding results found for ${entityType}.`);
                    return;
                }

                selectElement.innerHTML = '';

                if (data.length === 1) {
                    // Single result: Hide input, show display
                    const singleResult = data[0];
                    inputElement.style.display = 'none';
                    displayElement.textContent = `${singleResult.term.entry_name} (${singleResult.term.db}:${singleResult.term.id}, Score: ${singleResult.score.toFixed(2)})`;
                    displayElement.style.display = 'inline-block';
                    document.getElementById(tupleId).value = JSON.stringify([singleResult.term.db, singleResult.term.id]);
                    selectElement.style.display = 'none';
                } else {
                    // Multiple results: Create dropdown
                    data.forEach((result, index) => {
                        const option = document.createElement('option');
                        option.value = JSON.stringify({
                            source_db: result.term.db,
                            source_id: result.term.id,
                        });
                        option.textContent = `${result.term.entry_name} (${result.term.db}:${result.term.id}, Score: ${result.score.toFixed(2)})`;
                        if (index === 0) {
                            option.selected = true;
                            document.getElementById(tupleId).value = JSON.stringify([result.term.db, result.term.id]);
                        }
                        selectElement.appendChild(option);
                    });
                    selectElement.style.display = 'block';
                    inputElement.style.display = 'none';
                }

                groundButton.style.display = 'none';
                cancelButton.style.display = 'inline-block';

            } catch (error) {
                console.error(`Error grounding ${entityType}:`, error);
                alert(`An error occurred while grounding the ${entityType}.`);
            }
        }

        function cancelGrounding(inputElement, displayElement, selectElement, groundButton, cancelButton, tupleId) {
            inputElement.value = '';
            inputElement.style.display = 'inline-block';
            displayElement.textContent = '';
            displayElement.style.display = 'none';

            selectElement.innerHTML = '';
            selectElement.style.display = 'none';
            document.getElementById(tupleId).value = '';

            groundButton.style.display = 'inline-block';
            cancelButton.style.display = 'none';
        }

        groundAgentButton.addEventListener('click', function () {
            groundEntity(agentNameInput, agentDisplay, agentSelect, groundAgentButton, cancelAgentButton, 'agent-tuple', "Agent");
        });

        cancelAgentButton.addEventListener('click', function () {
            cancelGrounding(agentNameInput, agentDisplay, agentSelect, groundAgentButton, cancelAgentButton, 'agent-tuple');
        });

        groundOtherAgentButton.addEventListener('click', function () {
            groundEntity(otherAgentInput, otherAgentDisplay, otherAgentSelect, groundOtherAgentButton, cancelOtherAgentButton, 'other-agent-tuple', "Other Agent");
        });

        cancelOtherAgentButton.addEventListener('click', function () {
            cancelGrounding(otherAgentInput, otherAgentDisplay, otherAgentSelect, groundOtherAgentButton, cancelOtherAgentButton, 'other-agent-tuple');
        });

        groundMeshButton.addEventListener('click', function () {
            groundEntity(meshNameInput, meshDisplay, meshSelect, groundMeshButton, cancelMeshButton, 'mesh-tuple', "Mesh");
        });

        cancelMeshButton.addEventListener('click', function () {
            cancelGrounding(meshNameInput, meshDisplay, meshSelect, groundMeshButton, cancelMeshButton, 'mesh-tuple');
        });

        /* ========================== DROPDOWN CHANGE EVENT LISTENERS ========================== */
        agentSelect.addEventListener('change', function () {
            const selectedOption = agentSelect.options[agentSelect.selectedIndex];
            if (selectedOption) {
                const { source_db, source_id } = JSON.parse(selectedOption.value);
                document.getElementById('agent-tuple').value = JSON.stringify([source_db, source_id]);
            }
        });

        otherAgentSelect.addEventListener('change', function () {
            const selectedOption = otherAgentSelect.options[otherAgentSelect.selectedIndex];
            if (selectedOption) {
                const { source_db, source_id } = JSON.parse(selectedOption.value);
                document.getElementById('other-agent-tuple').value = JSON.stringify([source_db, source_id]);
            }
        });

        meshSelect.addEventListener('change', function () {
            const selectedOption = meshSelect.options[meshSelect.selectedIndex];
            if (selectedOption) {
                const { source_db, source_id } = JSON.parse(selectedOption.value);
                document.getElementById('mesh-tuple').value = JSON.stringify([source_db, source_id]);
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
            RelhiddenInput.value = JSON.stringify(selectedValues);
        });

        // Close the dropdown after selecting an option
        selectElement.addEventListener('choice', function () {
            choices.hideDropdown();
        });



        function resetExampleValues(agentText, otherAgentText, role, selectedChoiceValues) {
            // Clear selected items in Choices.js
            choices.removeActiveItems();

            // Reset agent and other agent inputs
            agentNameInput.value = agentText || '';
            otherAgentInput.value = otherAgentText || '';

            // Reset roles and button states
            roleButtons.forEach(button => button.classList.remove('active'));
            const targetRoleButton = document.getElementById(`btn-${role}`);
            if (targetRoleButton) targetRoleButton.classList.add('active');

            // Update agent roles
            agentRoleInput.value = role === 'subject' ? 'subject' : 'object';
            otherAgentRoleInput.value = role === 'subject' ? 'object' : 'subject';

            // Update labels and container visibility
            otherAgentLabeltext.textContent = 'Other Agent';
            otherAgentContainer.style.display = 'block';
            otherAgentContainer.style.marginTop = '10px';

            // Update Choices.js with selected values
            if (selectedChoiceValues && selectedChoiceValues.length > 0) {
                choices.setChoiceByValue(selectedChoiceValues);
                RelhiddenInput.value = JSON.stringify(selectedChoiceValues);
            }

            // Hide the cancel button and show the ground button
            cancelAgentButton.style.display = 'none';
            groundAgentButton.style.display = 'inline-block';
            cancelOtherAgentButton.style.display = 'none';
            groundOtherAgentButton.style.display = 'inline-block';
            cancelMeshButton.style.display = 'none';
            groundMeshButton.style.display = 'inline-block';

            // Show the text input
            agentNameInput.style.display = 'inline-block';
            agentDisplay.style.display = 'none';
            otherAgentInput.style.display = 'inline-block';
            otherAgentDisplay.style.display = 'none';
            meshNameInput.style.display = 'inline-block';
            meshDisplay.style.display = 'none';

            // Hide grounding result dropdowns
            agentSelect.style.display = 'none';
            otherAgentSelect.style.display = 'none';
            meshSelect.style.display = 'none';

            // Empty the tuple values
            document.getElementById('agent-tuple').value = '';
            document.getElementById('other-agent-tuple').value = '';
            document.getElementById('mesh-tuple').value = '';
        }

        exampleText1.addEventListener('click', function () {
            resetExampleValues('DUSP1', 'MAPK1', 'subject', []);
        });

        exampleText2.addEventListener('click', function () {
            resetExampleValues('CDK12', '', 'subject', ['Phosphorylation']);
        });

        exampleText3.addEventListener('click', function () {
            resetExampleValues('MTOR', '', 'object', ['Inhibition']);
        });

        exampleText4.addEventListener('click', function () {
            resetExampleValues('PIK3CA', '', 'either', []);
        });

        exampleText5.addEventListener('click', function () {
            resetExampleValues('seliciclib', '', 'subject', ['Inhibition']);
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


        // Hide the tooltip when clicking outside
        document.addEventListener('click', function (event) {
            if (!infoIcon.contains(event.target) && !tooltip.contains(event.target)) {
                tooltip.style.display = 'none';
            }
        });

    });