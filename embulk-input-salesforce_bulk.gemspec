
Gem::Specification.new do |spec|
  spec.name          = "embulk-input-salesforce_bulk"
  spec.version       = "0.2.1"
  spec.authors       = ["mikoto2000"]
  spec.summary       = %[Salesforce Bulk input plugin for Embulk]
  spec.description   = %[Loads records from Salesforce Bulk.]
  spec.email         = ["mikoto2000@gmail.com"]
  spec.licenses      = ["MIT"]
  spec.homepage      = "https://github.com/mikoto2000/embulk-input-salesforce_bulk"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r"^(test|spec)/")
  spec.require_paths = ["lib"]

  spec.add_development_dependency 'bundler', ['~> 1.0']
  spec.add_development_dependency 'rake', ['>= 10.0']
end
