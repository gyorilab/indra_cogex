document.addEventListener('DOMContentLoaded', function () {
        const agentNameInput = document.getElementById('agent-name');
        const otherAgentInput = document.getElementById('other-agent-name');
        const roleButtons = document.querySelectorAll('.btn-role');
        const agentRoleInput = document.getElementById('agent-role');
        const otherAgentRoleInput = document.getElementById('other-agent-role');
        const otherAgentContainer = document.getElementById('other-agent-container');
        const agentSelect = document.getElementById('agent-select');



        const stmtTypes = JSON.parse(document.getElementById('stmt-types-json').textContent);
        console.log('stmt-types-json element:', stmtTypes);
        const selectElement = document.getElementById('choices-multiple-remove-button');
        const hiddenInput = document.getElementById('rel-type-hidden');
        const groundAgentButton = document.getElementById('ground-agent-button');


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
            data.forEach(result => {
                const option = document.createElement('option');
                option.value = JSON.stringify({
                    source_db: result.term.source_db,
                    source_id: result.term.source_id,
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
            agentNameInput.value = `(${source_db}, ${source_id})`;
            agentSelect.style.display = 'none';
            agentNameInput.style.display = 'block';
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
    });