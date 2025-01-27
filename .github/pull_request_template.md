# Description

Include a summary of the change and which, if any, issue is fixed.

# Testing

Include instructions for how to test this change, where appropriate.

## Code Review Guide

If you are reviewing this PR, ensure you cover the below.

- **Functionality**: Does it work? Does it meet the requirements from Shortcut?
- <details>
  <summary><b>Readability and maintainability</b>: How easy is the code to read and maintain?</summary>

  - Does the logic make sense?
  - Can you understand what the code does on first read?
  </details>
- <details>
  <summary><b>Security</b>: Are there any security vulnerabilities? Does it meet the security considerations in Shortcut?</summary>
  
  - No API keys, secrets, or sensitive information in code
  - Correct permissions applied to available endpoints
  - No injection vulnerabilities, i.e SQL injection, XSS, command injection
  - Secure design
  - SSRF & CSRF
  - Appropriate logging and error handling
</details>

- **Performance and efficiency**: Is the code efficient?
- **Error handling and logging**: How does the code handle errors and logging?
- **Test coverage**: How well is the code covered by tests?
- **Code reuse and dependencies**: Does the code reuse existing functionality, or does it introduce new dependencies?
- **Documentation**: Is there adequate documentation?
- <details>
  <summary><b>Best practices</b>: Does the code adhere to best practices and design patterns?</summary>
  - Utilise framework functionality where possible/available
  - Use good and consistent variable, function and class names
  </details>
- <details>

By approving this PR you are confirming you have checked all of the above to a satisfactory level. 

