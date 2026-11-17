package n8n

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

//go:embed langchain.js
var langchainShim string

// langchainModules are the LangChain JS modules the shim provides.
var langchainModules = map[string]bool{
	"@langchain/core/prompts": true, "langchain/prompts": true, "@langchain/core/messages": true, "langchain/schema": true,
	"@langchain/core/output_parsers": true, "langchain/schema/output_parser": true, "@langchain/openai": true,
	"@langchain/qdrant": true, "@langchain/core/documents": true,
}

// langchainCodeNode runs a LangChain Code node's "execute" code. Connected
// chat models are available via this.getInputConnectionData.
func langchainCodeNode(ec *execCtx) ([][]Item, error) {
	code, _ := ec.str(0, "code.execute.code", "")
	if strings.TrimSpace(code) == "" {
		return nil, permanentError{"LangChain Code: only the Execute code mode is supported (Supply Data is not)"}
	}
	rt, err := ec.runtime()
	if err != nil {
		return nil, err
	}
	models := aiSubNodes(ec.node.Config, "ai_languageModel")
	if err := rt.vm.Set("__lcChat", func(idx int, msgsJSON string) string {
		if idx < 0 || idx >= len(models) {
			rt.throw("no chat model connected at index %d", idx)
		}
		model, req, err := ec.buildModelFor(models[idx], 0)
		if err != nil {
			rt.throw("%v", err)
		}
		var msgs []chatMsg
		_ = json.Unmarshal([]byte(msgsJSON), &msgs)
		req.Messages = msgs
		res, err := model.Chat(ec.ctx, req)
		if err != nil {
			rt.throw("%v", err)
		}
		return res.Content
	}); err != nil {
		return nil, err
	}
	if err := rt.vm.Set("__uuidV4", uuid.NewString); err != nil {
		return nil, err
	}
	if _, err := rt.vm.RunString(langchainShim); err != nil {
		return nil, fmt.Errorf("langchain shim: %w", err)
	}
	return runCode(ec, code, false, "")
}
