'use client';

import * as React from 'react';
import { useAuth } from '@/context/AuthContext';
import { api } from '@/lib/api';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Card, CardHeader, CardTitle, CardDescription, CardContent, CardFooter } from '@/components/ui/card';

export default function SettingsPage() {
    const { user } = useAuth();
    const [isLoading, setIsLoading] = React.useState(false);
    const [message, setMessage] = React.useState<{ type: 'success' | 'error', text: string } | null>(null);

    const handlePasswordChange = async (e: React.FormEvent<HTMLFormElement>) => {
        e.preventDefault();
        setIsLoading(true);
        setMessage(null);

        const formData = new FormData(e.currentTarget);
        const oldPassword = formData.get('oldPassword') as string;
        const newPassword = formData.get('newPassword') as string;
        const confirmPassword = formData.get('confirmPassword') as string;

        if (newPassword !== confirmPassword) {
            setMessage({ type: 'error', text: 'New passwords do not match' });
            setIsLoading(false);
            return;
        }

        try {
            await api.post('/users/change-password', {
                old_password: oldPassword,
                new_password: newPassword,
            }, { token: localStorage.getItem('token') || '' });

            setMessage({ type: 'success', text: 'Password updated successfully' });
            (e.target as HTMLFormElement).reset();
        } catch (err: any) {
            setMessage({ type: 'error', text: err.message || 'Failed to update password' });
        } finally {
            setIsLoading(false);
        }
    };

    return (
        <div className="space-y-6">
            <div>
                <h1 className="text-3xl font-bold tracking-tight">Settings</h1>
                <p className="text-muted-foreground">Manage your account settings and preferences.</p>
            </div>

            <div className="grid gap-6">
                <Card>
                    <CardHeader>
                        <CardTitle>Profile</CardTitle>
                        <CardDescription>Your account information.</CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-4">
                        <div className="grid gap-2">
                            <Label>Username</Label>
                            <Input value={user?.username || ''} disabled />
                        </div>
                        <div className="grid gap-2">
                            <Label>Role</Label>
                            <Input value={user?.role || ''} disabled />
                        </div>
                        <div className="grid gap-2">
                            <Label>Tier</Label>
                            <Input value={user?.tier || ''} disabled />
                        </div>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader>
                        <CardTitle>Change Password</CardTitle>
                        <CardDescription>Update your password to keep your account secure.</CardDescription>
                    </CardHeader>
                    <form onSubmit={handlePasswordChange}>
                        <CardContent className="space-y-4">
                            <div className="grid gap-2">
                                <Label htmlFor="oldPassword">Current Password</Label>
                                <Input id="oldPassword" name="oldPassword" type="password" required />
                            </div>
                            <div className="grid gap-2">
                                <Label htmlFor="newPassword">New Password</Label>
                                <Input id="newPassword" name="newPassword" type="password" required />
                            </div>
                            <div className="grid gap-2">
                                <Label htmlFor="confirmPassword">Confirm New Password</Label>
                                <Input id="confirmPassword" name="confirmPassword" type="password" required />
                            </div>
                            {message && (
                                <p className={`text-sm ${message.type === 'success' ? 'text-green-500' : 'text-red-500'}`}>
                                    {message.text}
                                </p>
                            )}
                        </CardContent>
                        <CardFooter>
                            <Button type="submit" isLoading={isLoading}>Update Password</Button>
                        </CardFooter>
                    </form>
                </Card>
            </div>
        </div>
    );
}
