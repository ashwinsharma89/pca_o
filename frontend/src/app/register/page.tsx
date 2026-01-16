import { RegisterForm } from '@/components/auth/RegisterForm';
import { Bot } from 'lucide-react';

export default function RegisterPage() {
    return (
        <div className="flex min-h-screen flex-col items-center justify-center bg-background p-4">
            <div className="mb-8 flex items-center gap-2 text-2xl font-bold">
                <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-primary/20 text-primary">
                    <Bot className="h-6 w-6" />
                </div>
                PCA Agent
            </div>
            <RegisterForm />
        </div>
    );
}
