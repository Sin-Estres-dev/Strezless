# Writing Clear Commit Messages for Strezless

## Why This Matters

When working on music business tools, clear commit history helps everyone understand what changed. Vague messages like "Initial plan" or "Update files" don't tell us if you fixed a royalty calculation, added a new split sheet template, or updated documentation.

## What Makes a Good Message

A helpful commit message answers: **What specific thing changed?**

### Structure

```
Brief summary of the change (aim for 50 characters or less)

Optional: Why this change was needed and any important context.
```

## Examples for Strezless Work

### Split Sheets & Templates
```
✅ Good:
Add 3-writer split sheet variant
Update percentage validation for split sheets
Fix total calculation in 5-writer template

❌ Avoid:
Update template
Fix bug
Changes
```

### Metadata & Documentation
```
✅ Good:
Document ISRC code requirements for distribution
Add PRO affiliation examples to quick-start guide
Clarify IPI number usage in artist profiles

❌ Avoid:
Update docs
Fix documentation
Add info
```

### Features & Functionality
```
✅ Good:
Enable export of split sheets to PDF format
Add revenue tracking fields to artist schema
Implement collaboration invite workflow

❌ Avoid:
New feature
Add stuff
Improvements
```

### Bug Fixes
```
✅ Good:
Fix royalty percentage rounding error
Correct ISNI validation pattern
Resolve duplicate artist name issue

❌ Avoid:
Bug fix
Fixes
Update
```

## Tips

1. **Be specific**: "Add 7-writer split template" beats "Add template"
2. **Use action verbs**: Add, Fix, Update, Remove, Implement
3. **Focus on the change**: What did you modify, not why you opened your editor
4. **Keep it short**: Aim for one line that captures the essence

## When to Add Details

Some changes need explanation. Add a blank line after your summary, then explain:

```
Migrate split sheet storage from JSON to database

The previous file-based approach caused sync issues when multiple
users edited sheets simultaneously. Moving to database storage
enables proper conflict resolution and audit trails.

Related to issue #42
```

## Questions?

If you're unsure whether your message is clear enough, ask yourself: "If I read this in 6 months, would I understand what changed?" If not, add more detail.
