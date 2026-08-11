import { codeSnippet, referenceSnippet } from '../components/VariablesTab';
import { IAfdagVariable } from '../ir';

const entry = (patch: Partial<IAfdagVariable> = {}): IAfdagVariable => ({
  key: 'api_base',
  scope: 'local',
  value: '',
  var_type: 'string',
  ...patch
});

describe('referenceSnippet', () => {
  it('uses var.value for a text variable', () => {
    expect(referenceSnippet(entry())).toBe('{{ var.value.api_base }}');
  });

  it('uses var.json for a JSON variable so Airflow deserializes it', () => {
    expect(referenceSnippet(entry({ var_type: 'json' }))).toBe(
      '{{ var.json.api_base }}'
    );
  });

  it('falls back to .get() when the key is not a plain identifier', () => {
    // Attribute access can't express a dotted/dashed key, but Airflow accepts
    // such keys, so the snippet has to switch form rather than emit something
    // that silently resolves to the wrong thing.
    expect(referenceSnippet(entry({ key: 'dotted.key' }))).toBe(
      '{{ var.value.get("dotted.key") }}'
    );
    expect(referenceSnippet(entry({ key: 'with-dash' }))).toBe(
      '{{ var.value.get("with-dash") }}'
    );
    expect(referenceSnippet(entry({ key: '9leading' }))).toBe(
      '{{ var.value.get("9leading") }}'
    );
  });
});

describe('codeSnippet', () => {
  it('emits a bare Variable.get for a text variable', () => {
    expect(codeSnippet(entry())).toBe('Variable.get("api_base")');
  });

  it('adds deserialize_json for a JSON variable', () => {
    expect(codeSnippet(entry({ var_type: 'json' }))).toBe(
      'Variable.get("api_base", deserialize_json=True)'
    );
  });

  it('uses the Airflow 3 SDK parameter name `default`, not `default_var`', () => {
    // `default_var` belongs to the Airflow 2 ORM Variable; against
    // airflow.sdk it raises TypeError at run time.
    const snippet = codeSnippet(entry({ default: 'fallback' }));
    expect(snippet).toBe('Variable.get("api_base", default="fallback")');
    expect(snippet).not.toContain('default_var');
  });

  it('combines deserialize_json and default', () => {
    expect(codeSnippet(entry({ var_type: 'json', default: '{}' }))).toBe(
      'Variable.get("api_base", deserialize_json=True, default="{}")'
    );
  });

  it('omits an empty default rather than emitting default=""', () => {
    expect(codeSnippet(entry({ default: '' }))).toBe(
      'Variable.get("api_base")'
    );
  });
});
