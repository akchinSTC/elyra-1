/*
 * Copyright 2018-2021 Elyra Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { FileParser } from '@elyra/services';
import { IconUtil, pyIcon, rIcon } from '@elyra/ui-components';
import { PathExt } from '@jupyterlab/coreutils';
import { DocumentRegistry } from '@jupyterlab/docregistry';
import { Contents } from '@jupyterlab/services';
import { LabIcon, notebookIcon } from '@jupyterlab/ui-components';
//import { stride } from '@lumino/algorithm';

import { PipelineService } from './PipelineService';

enum ContentType {
  notebook = 'notebook',
  python_script = 'python',
  r_script = 'r',
  other = 'other'
}

const CONTENT_TYPE_MAPPER: Map<string, ContentType> = new Map([
  ['.py', ContentType.python_script],
  ['.r', ContentType.r_script],
  ['.ipynb', ContentType.notebook]
]);

const ICON_MAPPER: Map<string, LabIcon> = new Map([
  ['.py', pyIcon],
  ['.r', rIcon],
  ['.ipynb', notebookIcon]
]);

export class CanvasManager {
  canvasController: any;
  widgetContext: DocumentRegistry.Context;

  constructor(widgetContext: DocumentRegistry.Context, canvasController: any) {
    this.widgetContext = widgetContext;
    this.canvasController = canvasController;
  }

  /**
   * Check if a given file is allowed to be added to the pipeline
   * @param item
   */
  isSupportedNode(file: Contents.IModel): boolean {
    if (CanvasManager.getNodeType(file.path)) {
      return true;
    } else {
      return false;
    }
  }

  async addNode(
    file: Contents.IModel,
    fileContent: string,
    x: number,
    y: number
  ): Promise<boolean> {
    console.log('Adding ==> ' + file.path);

    const nodeTemplate = this.canvasController.getPaletteNode(
      CanvasManager.getOperationName(file.path)
    );

    if (nodeTemplate) {
      const data = {
        editType: 'createNode',
        offsetX: x,
        offsetY: y,
        nodeTemplate: this.canvasController.convertNodeTemplate(nodeTemplate)
      };

      const env_vars = await FileParser.getEnvVars(file.path).then(response =>
        response.map((str: string) => (str = str + '='))
      );

      data.nodeTemplate.label = PathExt.basename(file.path);
      data.nodeTemplate.image = IconUtil.encode(
        CanvasManager.getNodeIcon(file.path)
      );
      data.nodeTemplate.app_data[
        'filename'
      ] = PipelineService.getPipelineRelativeNodePath(
        this.widgetContext.path,
        file.path
      );
      data.nodeTemplate.app_data['runtime_image'] = '';
      data.nodeTemplate.app_data['env_vars'] = env_vars;
      data.nodeTemplate.app_data['include_subdirectories'] = false;
      this.canvasController.editActionHandler(data);

      return true;
    } else {
      return false;
    }
  }

  static getOperationName(filepath: string): string {
    const type = CanvasManager.getNodeType(filepath);
    return `execute-${type}-node`;
  }

  static getNodeIcon(filepath: string): LabIcon {
    const extension: string = PathExt.extname(filepath);
    const icon: LabIcon = ICON_MAPPER.get(extension);

    // TODO: throw error when file extension is not supported?
    return icon;
  }

  // Private

  private static getNodeType(filepath: string): string {
    const extension: string = PathExt.extname(filepath);
    const type: string = CONTENT_TYPE_MAPPER.get(extension);

    // TODO: throw error when file extension is not supported?
    return type;
  }
}
