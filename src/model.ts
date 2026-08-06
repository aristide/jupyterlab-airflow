import { DocumentModel, DocumentRegistry } from '@jupyterlab/docregistry';
import { Contents } from '@jupyterlab/services';

import { IAfdagIR, createEmptyIR, stringifyIR } from './ir';

/**
 * The document model for a `.afdag` file. The IR graph is stored as JSON text
 * in the underlying shared model (a plain text/code model); helpers parse and
 * serialise it on demand. JupyterLab drives open/save/dirty/restore for us.
 */
export class AfdagModel extends DocumentModel {
  getIR(): IAfdagIR {
    const source = this.toString();
    if (!source.trim()) {
      return createEmptyIR('');
    }
    try {
      return JSON.parse(source) as IAfdagIR;
    } catch {
      return createEmptyIR('');
    }
  }

  setIR(ir: IAfdagIR): void {
    const next = stringifyIR(ir);
    if (next !== this.toString()) {
      this.fromString(next);
      this.dirty = true;
    }
  }
}

/**
 * Model factory binding `.afdag` documents to {@link AfdagModel}.
 */
export class AfdagModelFactory implements DocumentRegistry.IModelFactory<AfdagModel> {
  get name(): string {
    return 'afdag-model';
  }

  get contentType(): Contents.ContentType {
    return 'file';
  }

  get fileFormat(): Contents.FileFormat {
    return 'text';
  }

  get isDisposed(): boolean {
    return this._disposed;
  }

  dispose(): void {
    this._disposed = true;
  }

  preferredLanguage(path: string): string {
    return '';
  }

  createNew(options: DocumentRegistry.IModelOptions = {}): AfdagModel {
    // RTC is out of scope for v1, so we deliberately let the model create its
    // own standalone shared document rather than threading a collaborative one.
    return new AfdagModel({
      languagePreference: options.languagePreference,
      collaborationEnabled: options.collaborationEnabled
    });
  }

  private _disposed = false;
}
