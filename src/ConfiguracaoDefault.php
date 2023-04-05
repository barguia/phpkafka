<?php

namespace Barguia\Php74Rdkafka;

use RdKafka\Conf;

class ConfiguracaoDefault
{
    private array $configuracao;

    public function __construct(array $configuracao = array())
    {
        $this->configuracao = $configuracao;
        $this->addConfigAdicional($configuracao);
    }

    /**
     * @param Conf $conf
     * @return void
     */
    public function setConfiguracao(Conf $conf): void
    {
        foreach ($this->configuracao as $indice => $value) {
            if ($value !== null) {
                $conf->set($indice, $value);
            }
        }
    }

    /**
     * @param array $configAdicional
     * @return void
     */
    private function addConfigAdicional(array $configAdicional): void
    {
        foreach ($configAdicional as $indice => $value) {
            if ($value !== null) {
                $this->configuracao[$indice] = $value;
            }
        }
    }

    /**
     * @return array
     */
    public function getConfiguracaoDefault(): array
    {
        return $this->configuracao;
    }
}
