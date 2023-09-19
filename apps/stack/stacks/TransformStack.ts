import {
  use,
  Config,
  type StackContext,
} from "sst/constructs";
import { ExtractStack } from "./ExtractStack";

export function TransformStack({ stack }: StackContext) {

  const { ExtractBus } = use(ExtractStack);
  const TRANSFORM_DB_URL = new Config.Secret(stack, "TRANSFORM_DB_URL");
  const TRANSFORM_DB_TOKEN = new Config.Secret(stack, "TRANSFORM_DB_TOKEN");

  ExtractBus.addRules(stack, {
    "transformRepository": {
      pattern: {
        source: ["extract"],
        detailType: ["repository"]
      },
      targets: {
        transformRepository: {
          function: {
            bind: [TRANSFORM_DB_URL, TRANSFORM_DB_TOKEN],
            handler: "src/transform/transform-repository.eventHandler",
          }
        }
      },
      
    }
  });

}